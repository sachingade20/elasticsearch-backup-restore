Install requirements using 
	$pip install -r requirements.txt


	$ python elasticsearch_utility.py --help
 
	usage: elasticsearch_utility.py [-h] [--config-path CONFIG_PATH]
                                        [--environment ENVIRONMENT] [--action ACTION]
                                        [--restore-date RESTORE_DATE]
                                        [--retention-days RETENTION_DAYS]
                                        [--cleanup-snapshot-date CLEANUP_SNAPSHOT_DATE]
         
        optional arguments:
          -h, --help            show this help message and exit
          --config-path CONFIG_PATH
                                elasticsearch_backup.yml
          --environment ENVIRONMENT
                                environment to perform: staging, production
          --action ACTION       Action to Perform backup, restore, cleanup_index,
                                cleanup_snapshot
          --restore-date RESTORE_DATE
                                Only applicable if action: restore Date to Restore
                                Snapshot. in YYYY.m.d format e.g 2018.04.25 you can
                                specify range as well <start-date>-<end-date>
                                2018.04.15-2018.04.20
          --retention-days RETENTION_DAYS
                                No Of Days to Keep Indices in Cleanup Job. Cleanup
                                action will cleanup all indices older than this days
          --cleanup-snapshot-date CLEANUP_SNAPSHOT_DATE
                                Only applicable if action: cleanup_snapshot Date to
                                Delete Snapshot. in YYYY.m.d format e.g 2018.04.25

