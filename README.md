# pm2es
fetch data from pmacct, send it to elasticsearch:
```bash
sudo pmacct -l -p /var/spool/pmacct/sfacctd_mem.pipe -s -O json -e | pm2es.py
```
