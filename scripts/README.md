# Automation scripts

Bash scrips to automate scraper and data pipeline execution using [`cron`](https://www.man7.org/linux/man-pages/man8/cron.8.html). These files are ment to be run from Mansueto Institute's cloud servers daily at 5:30am UTC. They can be used as reference to automate on different machines.

## Configure Cron Jobs 
1. Copy contents from `crontab_tasks.txt` or specify your own periodicity with [crontab syntax](https://linuxhandbook.com/crontab/)
1. Open crontab and select preferred editor from your terminal with `$ crontab -e`
1. Paste contents on crontab and exit editor
1. Check jobs with `$ crontab -l`