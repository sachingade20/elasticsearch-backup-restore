import argparse
import sys
import boto3
import botocore
import time
import yaml
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta, MO
import requests
import json
import elasticsearch
import curator

ELASTICSEARCH_URL = "elasticsearch_url"
BUCKET_NAME = "bucket_name"
REGION = "region"
BASE_PATH = "base_path"
START_DATE = "start_date"
SNAPSHOTS = 'snapshots'
BACKUP_METADATA_FILE = "backup_metadata.yml"
BACKUP_METADATA = "config/backup_metadata.yml"
BACKUP_INDEX = "backup_index"
def parse_args(argv):
    """
    Parse Args
    :param argv:
    :return:
    """
    args = argv
    if '--' in argv:
        args = argv[:argv.index('--')]

    parser = argparse.ArgumentParser()
    parser.add_argument('--config-path', default='config/elasticsearch_backup.yml',
                        help='elasticsearch_backup.yml')
    parser.add_argument('--environment', default='staging',
                        help='environment to perform: staging, production')
    parser.add_argument('--action', default='backup',
                        help='Action to Perform backup, restore, cleanup_index, cleanup_snapshot')
    parser.add_argument('--restore-date', default=None,
                        help='Only applicable if action: restore Date to Restore Snapshot. in YYYY.m.d format e.g 2018.04.25 you can specify range as well '
                             '<start-date>-<end-date> 2018.04.15-2018.04.20')
    parser.add_argument('--retention-days', default='15',
                        help='No Of Days to Keep Indices in Cleanup Job. Cleanup action will cleanup all indices older than this days')
    parser.add_argument('--cleanup-snapshot-date', default="2018.04.10",
                        help='Only applicable if action: cleanup_snapshot Date to Delete Snapshot. in YYYY.m.d format e.g 2018.04.25 ')
    parser.add_argument('--start-date', default=None,
                        help='Only applicable if action: backup YYYY.mm.dd  Backup Process will try to snapshot from provided date. ')

    args = parser.parse_args(args[1:])
    return args


def download_metadata(configs):
    print "Download Metadata"
    s3 = boto3.resource('s3')

    try:
        bucket  = s3.Bucket(configs['bucket_name'])
        with open(BACKUP_METADATA, 'wb') as data:
            bucket.download_fileobj("%s/%s" % (configs[BASE_PATH],BACKUP_METADATA_FILE), data)
        with open(BACKUP_METADATA, "r") as fd:
            snapdata = fd.read()
            metadata = yaml.load(snapdata)
        print "Metadata Downloaded %s" % metadata
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
            with open(BACKUP_METADATA + ".template", "r") as fd:
                snapdata = fd.read()
                metadata = yaml.load(snapdata)
        else:
            raise

    if not metadata:
        return {"snapshots": {}, "last_good": {}}
    else:
        return metadata


def upload_metadata(metadata, configs):
    print "Upload Metadata"
    s3 = boto3.resource('s3')
    metadata['configs'] = configs
    with open(BACKUP_METADATA, "w") as fd:
        fd.write(yaml.dump(metadata))
    try:
        bucket = s3.Bucket(configs[BUCKET_NAME])
        bucket.upload_file(BACKUP_METADATA, "%s/%s" % (configs[BASE_PATH],BACKUP_METADATA_FILE))
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
            return False
        else:
            raise
    return True


def create_snapshot_repository(repository_name, configs, metadata):
    if 'snapshots' in metadata and repository_name in metadata['snapshots']:
        print("Repo Already Exist")
        return
    url = "%s/_snapshot/%s" % (configs[ELASTICSEARCH_URL], repository_name)
    querystring = {"verify":"false","pretty":""}
    payload_settings = {
        "bucket": configs[BUCKET_NAME],
        "region": configs[REGION],
        "base_path": "%s/%s" % (configs[BASE_PATH], repository_name)
    }
    payload = {
        "type": "s3",
        "settings": payload_settings
    }
    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache",
        }
    response = requests.request("PUT", url, data=json.dumps(payload), headers=headers, params=querystring)
    if response.status_code  == 200:
        print("Repo Created/Exist")
    else:
        raise Exception("Failed to Create Snapshot Repository. Status Received %s" % str(response.text))
    if 'snapshots' not in metadata:
        metadata['snapshots'] = {}
    metadata['snapshots'][repository_name] = []


def get_repository_name_for_week(backup_day):
    last_monday = backup_day + relativedelta(weekday=MO(-1))
    print last_monday.strftime("%-m-%d")
    return "elasticsearch-backup-%s" % last_monday.strftime("%Y.%m.%d")


def is_snapshot_exist(metadata, configs, repository_name, snapshot_name):
    if 'snapshots' in metadata and repository_name in metadata['snapshots']:
        print("Repository Exist")
        if snapshot_name in metadata['snapshots'][repository_name]:
            print "Snapshot Exist"
            return True
    return False


def get_snapshot_and_indices(configs, backup_day):
    indices_name = "%s%s" % (configs[BACKUP_INDEX], backup_day.strftime("%Y.%m.%d"))
    snapshot_name = "snapshot-%s" % indices_name
    return snapshot_name, indices_name

def create_snapshots_for_week(metadata, configs, backup_day):
    print "creating snapshot for %s" % backup_day
    repository_name = get_repository_name_for_week(backup_day)
    create_snapshot_repository(repository_name, configs, metadata)
    print "repository_name: ", repository_name
    snapshot_name, indices_name = get_snapshot_and_indices(configs, backup_day)
    if not is_snapshot_exist(metadata, configs, repository_name, snapshot_name):
        print "Creating Snapshot %s in Repository %s" % (snapshot_name, repository_name)
        if is_indices_exist(configs, indices_name):
            create_snapshot(metadata, configs, snapshot_name, repository_name, indices_name)
        else:
            print("Indices Does Not Exist %s" % indices_name)


def create_snapshot(metadata, configs, snapshot_name, repository_name, indices_name):
    url = "%s/_snapshot/%s/%s" % (configs[ELASTICSEARCH_URL], repository_name, snapshot_name)
    querystring = {"pretty":"","wait_for_completion":"true"}
    payload = {
        "indices" : "%s" % indices_name,
        "ignore_unavailable": True,
        "include_global_state": False
    }
    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache"
        }
    response = requests.request("PUT", url, data=json.dumps(payload), headers=headers, params=querystring)
    if response.status_code == 200:
        print "Created Snapshot %s in Repository %s" % (snapshot_name, repository_name)
    elif response.status_code == 504:
        print "Snapshot is in Progress Wait for sometime and continue..."
        time.sleep(180)
    else:
        raise Exception("Failed to Create Snapshot. Status Received %s" % str(response.text))

    metadata['snapshots'][repository_name].append(snapshot_name)
    metadata['last_good'] = {repository_name: snapshot_name}


def create_snapshots_from_start_date(metadata, configs):
    print "Creating snapshot from start date"
    start_date = configs[START_DATE]
    start = datetime.strptime(start_date, '%Y.%m.%d')
    end = datetime.today()
    for date in daterange(start, end ):
        backup_day = date - timedelta(1)
        create_snapshots_for_week(metadata , configs, backup_day)


def daterange(start_date, end_date ):
    if start_date != end_date:
        for n in range( ( end_date - start_date ).days + 1 ):
            yield start_date + timedelta( n )
    else:
        for n in range( ( start_date - end_date ).days + 1 ):
            yield start_date - timedelta( n )


def is_indices_exist(configs, indices):
    url = "%s/%s" % (configs[ELASTICSEARCH_URL], indices)

    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache",
        }

    response = requests.request("HEAD", url, headers=headers)
    if response.status_code == 200:
        print("Indices Exist %s" % indices)
        return True

    return False


def delete_snapshots(configs, repository_name, snapshot_name):
    print "Deleting Snapshot  %s" % snapshot_name
    url = "%s/_snapshot/%s/%s" % (configs[ELASTICSEARCH_URL], repository_name, snapshot_name)
    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache"
        }
    response = requests.request("DELETE", url, headers=headers)

    print(response.text)


def perform_backup(configs):
    print "Perform Backup"
    metadata = download_metadata(configs)
    print "Metadata: ", metadata

    if configs[START_DATE]:
        create_snapshots_from_start_date(metadata, configs)
    else:
        backup_day = date.today() - timedelta(1)
        create_snapshots_for_week(metadata, configs, backup_day)
    upload_metadata(metadata, configs)
    print "Backup Completed Successfully"


def perform_snapshot_cleanup(configs):
    print "Perform Cleanup"
    metadata = download_metadata(configs)
    snapshot_date = configs['cleanup_snapshot_date']
    if snapshot_date == "all":
        if 'snapshots' in metadata:
            for key, value in metadata['snapshots'].iteritems():
                print "Cleaning Up Repository %s" % key
                for snapshot in value:
                    delete_snapshots(configs, key, snapshot )
        metadata['snapshots'] = {}
        metadata['last_good'] = {}
        upload_metadata(metadata, configs)
    else:
        backup_day = datetime.strptime(snapshot_date, '%Y.%m.%d')
        snapshot_name, indices_name = get_snapshot_and_indices(configs, backup_day)
        repository_name = get_repository_name_for_week(backup_day)
        if is_snapshot_exist(metadata,configs, repository_name, snapshot_name):
            delete_snapshots(configs, repository_name, snapshot_name)
            metadata['snapshots'][repository_name].remove(snapshot_name)
    upload_metadata(metadata,configs)


def perform_cleanup(configs):

    client = elasticsearch.Elasticsearch(hosts=configs['elasticsearch_url'], port=80)
    ilo = curator.IndexList(client)
    ilo.filter_by_regex(kind='prefix', value=configs['backup_index'])
    days = configs['backup_retention_days']
    if days:
        ilo.filter_by_age(source='name', direction='older', timestring='%Y.%m.%d', unit='days', unit_count=int(days))
        delete_indices = curator.DeleteIndices(ilo)
        delete_indices.do_action()
        print "Cleaned up Indices  %s" % delete_indices
    print "Cleaned up Indices  Completed Successfully"

def restore_snapshot(configs, repository_name, snapshot_name, indices_name):
    url = "%s/_snapshot/%s/%s/_restore" % (configs[ELASTICSEARCH_URL], repository_name, snapshot_name)
    payload = {
        "indices" : indices_name,
        "ignore_unavailable": True,
        "include_global_state": False,
    }
    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache"
        }

    response = requests.request("POST", url, data=json.dumps(payload), headers=headers)
    if response.status_code == 200:
        print "Restored Snapshot %s  with Renamed %s" % (snapshot_name, repository_name)
    else:
        raise Exception("Failed to Restore Snapshot. Status Received %s" % str(response.text))


def restore_snapshots_for_day(metadata , configs, backup_day):
    repository_name = get_repository_name_for_week(backup_day)
    snapshot_name, indices_name = get_snapshot_and_indices(configs, backup_day)
    if is_snapshot_exist(metadata, configs, repository_name, snapshot_name):
        restore_snapshot(configs, repository_name, snapshot_name, indices_name)


def perform_restore(configs):
    metadata = download_metadata(configs)
    restore_date = configs['restore_date']
    if restore_date:
        sdate = restore_date.split('-')
        if len(sdate) == 2:
            start_date =  datetime.strptime(sdate[0], '%Y.%m.%d')
            end_date = datetime.strptime(sdate[1], '%Y.%m.%d')
            for backup_day in daterange(start_date, end_date):
                restore_snapshots_for_day(metadata , configs, backup_day)
        else:
            restore_snapshots_for_day(metadata , configs, datetime.strptime(sdate[0], '%Y.%m.%d'))
    print "Restore Completed for %s" % restore_date


def load_configs(args):
    print "Loading Configs"
    with open(args.config_path, "r") as fd:
        data = fd.read()
        config_data = yaml.load(data)
        if args.environment in config_data:
            return config_data[args.environment]
    raise Exception("Invalid Config File or Environment")


def main(args):
    print "Running backup_and_restore utility with environment %s" % args.environment
    configs = load_configs(args)
    if args.action == "backup":
        if args.start_date:
            configs['start_date'] = args.start_date
        perform_backup(configs)
    elif args.action == "restore":
        if args.restore_date:
            configs['restore_date'] = args.restore_date
        perform_restore(configs)
    elif args.action == "cleanup_index":
        if args.retention_days:
            configs['backup_retention_days'] = args.retention_days
        perform_cleanup(configs)
    elif args.action == "cleanup_snapshot":
        if args.cleanup_snapshot_date:
            configs['cleanup_snapshot_date'] = args.cleanup_snapshot_date
        else:
            print "Snapshot Cleanup Date is Not Provided... Exiting"
            exit(0)
        perform_snapshot_cleanup(configs)
    else:
        print("Invalid Action specified")


if __name__ == '__main__':
    args = parse_args(sys.argv)
    main(args)
