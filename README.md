# NOMA

![NOMA API ORM](noma.png)

## NOMA API ORM Specification

![NOMA API ORM](noma_api.png)

## Getting Started

These instructions will get you a copy of the project up and running on your local machine (Windows 7 or later) for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

	1. Install Notepad++, add NppExec plugin
	2. Install Mariadb & HeidiSQL
	3. Create Database nomadb & xnaxdr (for multi-tenent create nomadb1 & xnaxdr1)
	4. Install Python 3.6.3
	5. Upgrade pip (py -m pip install --upgrade pip)
	6. install redis (Redis-x64-3.2.100.msi) for Windows (run as service)
	   https://github.com/MicrosoftArchive/redis/releases
	7. Install Git (git-scm.com)


### Installing

	8. Git clone github.com/ckhu2020/xna2
	9. Install virtualenv (py -m pip install --user virtualenv)
	10. Create virtual environment named sera_venv in folder pyenvs/sera/
	11. Activate virtual environment sera_venv (pyvenvs/sera/sera_venv/Scripts/activate)
	12. cd to /sera/xna2, run pip install -r noma_dependencies.xtx
	13. To execute NOMA scripts in Notepad++, add in NppExec Execute...
	    cmd /k C:\pyvenvs\sera\sera_venv\Scripts\activate & python "$(FILE_NAME)" & deactivate & exit
	14. Download dash_pivottable from https://github.com/xhlulu/dash_pivottable. Copy dash_pivottable folder to 
	    pyvenvs/sera/sera_venv/Lib/site-packages
	15. Create data folder (sera/data/nomasftp/uploads & downloads)
	16. Update settings.ini (using settings.ini.example as template)
	17. Create a migrations folder under xna2/noma and create an empty  __init__.py inside
	18. Launch Powershell, activate sera_venv, Run python manage.py makemigrations,  python manage.py migrate
	19. Run HeidiSQL to check NOMA ORM tables are created
	20. Under sera_venv, Run python manage.py createsuperuser
	21. Under sera_venv, Run python manage.py collectstatic 
	    (this will collect all noma static files under settings.STATIC_ROOT)
	22. Under sera_venv, Run python manage.py runserver
	23. Launch anonther Powershell, activate sera_venv, Run celery -A xna2 worker -l info
	24. Open Chrome browser: 127.0.0.1:8000/noma, login using superuser created in step-20
	25. In developer mode (i.e. runserver and DEBUG=True), copy settings.STATIC_ROOT/dash to xna2/noma/static/dash
	26. Run NOMA Query Group Dash_Pivot_Datasets, if Dash Pivottable is not loading, use Chrome developer tool 
	    (ctrl+shift+i) to check the console errors and 
	    correct the dash static files versioning in xna2.noma/static/dash according to the console errors mesages


## Deployment for Production

NOMA can be deployed as cloud-based service. Follow the step-by-step deployment guides below to deploy NOMA on a Linux (CentOS 7) VM.

### 1. Setting up a Linux VM with Python installed
	1.1. Secure a Linux VM (min spec: 4 vCPU, 12GB RAM, 200 GB vDisk) with CentOS 7 image.
	1.2. Ensure that the Linux VM has Internet Access and has an IP address that is reachable by the targeted 
	     NOMA users.
	1.3. Optionally (and preferably), obtain a host name for your Linux VM and the associated DNS resolution
	1.4. Login as root or superuser. 
	1.5. Mount vDisk volumne at '/mnt' (e.g. mount /dev/vdb /mnt in /etc/fstab)
	1.6. Enable Software Collections(SCL)
	     $sudo yum install centos-release-scl
	1.7. Install Python 3.6 on Centos
	     $sudo yum install rh-python36
	1.8. python --version shows default python 2.7.5
         To access to Python 3.6 for the current session
         $scl enable rh-python36 bash
	1.9. Install Development Toos
	     $sudo yum groupinstall 'Development Tools'	

### 2. Setting up Git Repositiry for your NOMA Project Fork 
	2.1. Create Project Fork Git Repository at NOMA Server
		$useradd git
		$su git
		$mkdir .ssh
		$chmod 700 .ssh
		$touch .ssh/authorized_keys
		$chmod 600 .ssh/authorized_keys
		 -paste NOMA Developer's public key to authorized_keys
		$exit (back to root)
		$mkdir /mnt/git
		$mkdir sera
		$cd /mnt/sera
		$mkdir xna2.git
		$cd xna2.git
		$git init --bare --shared
		$chown -R root:git xna2.git

	2.2. Push NOMA project from development pc (follow the Getting Started for setting up the NOMA Developer PC)
		Launch git bash
		$pwd - to check home directory (e.g. Users/username)
		 -Export user's private to OpenSSH key (PuTTYgen -> Conversion -> Export OPenSSH key), 
		  save it to HOME\.ssh\id_rsa
		$cd /c/sera/xna2
		$git status (commit last changes if needed)
		$git remote add noma_project_repo git@<NOMA Server IP>:/mnt/git/sera/xna2.git
		$git push noma_project_repo master

	2.3. Clone NOMA project to production NOMA Server
		login to NOMA Server
		$mkdir /mnt/sera
		$git clone /mnt/git/sera/xna2.git /mnt/sera/xna2
		$cd mnt/sera/xna2
		$ls
		$git pull

	2.4. disbale git for bash shell
		$cat /etc/shells # if git-shell not there
		$which git-shell
		$nano /etc/shells (add which git-shell to the bottom)
		$chsh git -s <which git shell>

### 3. Setting Up MariaDB Server

	3.1. Enable the MariaDB repository. Create a repository file named MariaDB.repo and add the following content:
		/etc/yum.repos.d/MariaDB.repo
		[mariadb]
		name = MariaDB
		baseurl = http://yum.mariadb.org/10.3/centos7-amd64
		gpgkey=https://yum.mariadb.org/RPM-GPG-KEY-MariaDB
		gpgcheck=1

	3.2. Install the MariaDB server and client packages using yum, same as other CentOS package:
		$yum install MariaDB-server MariaDB-client MariaDB-shared

		Yum may prompt you to import the MariaDB GPG key:
		Retrieving key from https://yum.mariadb.org/RPM-GPG-KEY-MariaDB
		Importing GPG key 0x1BB943DB:
		Userid     : "MariaDB Package Signing Key <package-signing-key@mariadb.org>"
		Fingerprint: 1993 69e5 404b d5fc 7d2f e43b cbcb 082a 1bb9 43db
		From       : https://yum.mariadb.org/RPM-GPG-KEY-MariaDB

		Type y and hit Enter.

		if Transaction Error on Conflict Files, use yum remove <conflicted-pacakage>. Example: yum remove mariadb-common*

	3.3. Once the installation is complete, enable MariaDB to start on boot and start the service:
		$sudo systemctl enable mariadb
		$sudo systemctl start mariadb

		To verify the installation check the MariaDB service status by typing:

		$sudo systemctl status mariadb

		? mariadb.service - MariaDB 10.3.7 database server
		Loaded: loaded (/usr/lib/systemd/system/mariadb.service; enabled; vendor preset: disabled)
		Drop-In: /etc/systemd/system/mariadb.service.d
			+-migrated-from-my.cnf-settings.conf
		Active: inactive (dead)
		Docs: man:mysqld(8)
			https://mariadb.com/kb/en/library/systemd/

	3.4. The last step is to run the mysql_secure_installation script which will perform several security related tasks:

		$mysql_secure_installation

		The script will prompt you to set up the root user password, remove the anonymous user, restrict root user access to the local machine, and remove the test database.

		All steps are explained in detail and it is recommended to answer Y (yes) to all questions. 
		For security reason, disable root login from remote host.

	3.5. Connect to MariaDB from the command line
		To connect to the MariaDB server through the terminal as the root account type:

		$mysql -u root -p

	3.6. Create User Account and Grant Privileges:
		To create a user that can connect from any host, use the '%' wildcard as a host part:

		CREATE USER 'newuser'@'%' IDENTIFIED BY 'user_password';

		Grand all privileges to a user account over all databases:

		GRANT ALL PRIVILEGES ON *.* TO 'database_user'@'localhost';

		Connect to MariaDB Server using HeidiSQL using the created user (open port 3306 on Firewall).


### 4. Getting NOMA Server off the ground
	4.1. Creat Virtual Environment for project SERA:
		$mkdir /usr/venvs/sera
		$cd /usr/venvs/sera
		$scl enable rh-python36 bash
		$python -m venv sera_venv
		$source sera_venv/bin/activate
		$cd /mnt/sera/xna2
		$pip install -r noma_dependencies.txt
	4.2. modify settings.ini.example with production enviroment variables. save as settings.ini
	4.3. Create a migrations folder under xna2/noma and create an empty  __init__.py inside
	4.4. activate sera_venv, Run python manage.py makemigrations,  python manage.py migrate
	4.5. Run HeidiSQL connect to MariaDB Server to check NOMA ORM tables are created
	4.6. Under sera_venv, Run python manage.py createsuperuser
	     (this will collect all noma static files under settings.STATIC_ROOT)
	4.8. Under sera_venv, Run python manage.py runserver (at port 80)
	4.9. Check NOMA Server is running on runserver (http://<noma_fqdn>/noma)
	4.10. stop the runserver (ctrl-c)


### 5. Setting up gunicorn
   (https://www.digitalocean.com/community/tutorials/how-to-set-up-django-with-postgres-nginx-and-gunicorn-on-centos-7)

	5.1. Under sera_venv, install gunicorn (if not alreay in noma_dependencies.txt) 
		$pip install gunicorn
	5.2. check gunicorn is working
		$gunicorn --bind 0.0.0.0:80 xna2.wsgi:application 
		check NOMA is running on gunicorn (http://<noma_fqdn>/noma) (no static)
		stop gunicorn (ctrl-c)
	5.3. create gunicorn systemd
		nano /etc/systemd/system/gunicorn.service

			[Unit]
			Description=gunicorn daemon
			After=network.target

			[Service]
			User=root
			Group=nginx
			WorkingDirectory=/mnt/sera/xna2
			ExecStart=/usr/venvs/sera/sera_venv/bin/gunicorn --workers 3 --bind unix:/mnt/sera/xna2/xna2.sock xna2.wsgi:application

			[Install]
			WantedBy=multi-user.target

	5.4. start and enbale gunicorn service
		 $systemctl start gunicorn
		 $systemctl enable gunicorn
		 $systemctl status gunicorn


### 6. Setting up Nginx
	6.1. Install Nginx
		$yum install nginx

	6.2. check static_root setting for static files location
	     STATIC_ROOT = os.path.join(BASE_DIR, "static/")
	6.3. Under sera_venv, Run python manage.py collectstatic to collect all static file into STATIC_ROOT directory


	6.4. Modify nginx config file (adding NOMA server)
		nano /etc/nginx/nginx.conf


		server {
			listen 80;
			server_name 10.164.196.144
						noma001.dyn.nesc.nokia.net
						;
			location = /favicon.ico { access_log off; log_not_found off; }
			location /static/ {
				root /mnt/sera/xna2;
			} 
     
			location / {
				proxy_set_header Host $http_host;
				proxy_set_header X-Real-IP $remote_addr;
				proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
				proxy_set_header X-Forwarded-Proto $scheme;
				proxy_pass http://unix:/mnt/sera/xna2/xna2.sock;
			}
	}

	6.5. add nginx user to root group
		$usermod -a -G root nginx

	6.6. test nginx 
		$nginx -t

	6.7. start and enbale nginx
		$systemctl start nginx
		$systemctl enable nginx
		$systemctl status nginx

	6.8. check NOMA is running on nginx (http://<noma_fqdn>/noma) 


### 7. Setting up supervisord for celery -A xna2 worker -l info
   (https://computingforgeeks.com/configure-celery-supervisord-centos-7-django-virtualenv/)

	7.1. Install Redis
		$yum install redis
		$systemctl start redis
		$systemctl enable redis
	
	7.2. Verify that Redis is running with redis-cli:
		$redis-cli ping

	7.3. Install supervisod
		$yum install supervisor

	7.4. create celery config file for xna2
		$nano /etc/supervisord.d/xna2.ini

			[program:xna2]
			command=/usr/venvs/sera/sera_venv/bin/celery -A xna2 worker -l info
			directory=/mnt/sera/xna2
			user=noma
			numprocs=1
			stdout_logfile=/mnt/sera/logs/celery-worker.log
			stderr_logfile=/mnt/sera/logs/celery-worker.log
			autostart=true
			autorestart=true
			startsecs=10

			; Need to wait for currently executing tasks to finish at shutdown.
			; Increase this if you have very long running tasks.
			stopwaitsecs = 600

			; When resorting to send SIGKILL to the program to terminate it
			; send SIGKILL to its whole process group instead,
			; taking care of its children as well.
			killasgroup=true

			; if rabbitmq is supervised, set its priority higher
			; so it starts first
			priority=998


	7.5. Start supervisor
		$systemctl start supervisord

	7.6. check & restart supervor for xna2 
		$supervisorctl status xna2
		$supervisorctl restart xna2

	7.7. To check NOMA Server Task Execution 
		$tail -f /mnt/sera/logs/celery-worker.log
		

### 8. Setting up NOMA SFTP Server
	8.1. Create SFTP Group and User - nomasftp
		$groupadd sftpgrp
		$useradd -g sftpgrp nomasftp

	8.2. Create SFTP Directory for user nomasftp
		$mkdir /mnt/noma/sftp/nomasftp mkdir /mnt/noma/sftp/nomasftp
		$mkdir /mnt/noma/sftp/nomasftp/.ssh
		$mkdir /mnt/noma/sftp/nomasftp/uploads
		$mkdir /mnt/noma/sftp/nomasftp/downloads
		$chown nomasftp:sftpgrp /mnt/noma/sftp/uploads
		$chown nomasftp:sftpgrp /mnt/noma/sftp/downloads
		$chmod 700 /mnt/noma/sftp/nomasftp/uploads
		$chmod 700 /mnt/noma/sftp/nomasftp/downloads
		$chmod 700 /mnt/noma/sftp/nomasftp/.ssh

	8.3. Modify sshd_config
		$nona /etc/ssh/sshd_config

			Match Group sftpgrp
			  ChrootDirectory /mnt/noma/sftp/%u
			  AuthorizedKeysFile /mnt/noma/sftp/%u/.ssh/authorized_keys
			  ForceCommand internal-sftp

	    $ststemctl restart sshd

	8.4. paste user's public key into .ssh/authorized_keys
		$nano /mnt/noma/sftp/nomasftp/.ssh/authorized_keys
		$chmod 700 /mnt/noma/sftp/nomasftp/.ssh/authorized_keys

	8.5. Ask NOMA users to connect to NOMA SFTP server using winSCP with their own private keys

	8.6. Add second tenant:
		$useradd -g sftpgrp nomasftp1
		$mkdir /mnt/noma/sftp/nomasftp1
		$mkdir /mnt/noma/sftp/nomasftp1/.ssh
		$mkdir /mnt/noma/sftp/nomasftp1/uploads
		$mkdir /mnt/noma/sftp/nomasftp1/downloads
		$chown nomasftp1:sftpgrp /mnt/noma/sftp/nomasftp1/uploads
		$chown nomasftp1:sftpgrp /mnt/noma/sftp/nomasftp1/downloads
		$chown nomasftp1:sftpgrp /mnt/noma/sftp/nomasftp1/.shh
		$chmod 700 /mnt/noma/sftp/nomasftp1/uploads
		$chmod 700 /mnt/noma/sftp/nomasftp1/downloads
		$chmod 700 /mnt/noma/sftp/nomasftp1/.ssh
		$nano /mnt/noma/sftp/nomasftp/.ssh/authorized_keys
		$chown nomasftp1:sftpgrp /mnt/noma/sftp/nomasftp1/.shh//authorized_keys
		$chmod 600 /mnt/noma/sftp/nomasftp1/.ssh/authorized_keys


## Contributing

Should you wish to contribute to this repository, please first discuss the change you wish to make via issue, email, or any other method with the owners of this repository


## Authors

* **Hu Chee Kiong** - *Initial work* - [CHEEHU](https://github.com/CHEEHU)


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details


## Acknowledgments

NOMA is standing on the shoulders of giants. All the heavylifiting are performed by the giants listed (but not limited to) below:

* Python ecosystem (for the full list of depenencies, see [noma_dependencies.txt](noma_dependencies.txt) file)
* MariaDB and HeidiSQL
* Django Web Framework
* Plotly-Dash and react-pivottable
* Nginx and Gunicorn
* CentOS
* Notepad++ with NppExec plugin
* Celery + Redis
* git-scm.com


NOMA relies heavily on codes contributed by others in the open source community, special thanks to:

1. Django-Plotly-Dash and react-pivottable:
   * https://github.com/plotly/react-pivottable
   * https://github.com/xhlulu/dash_pivottable
   * https://github.com/GibbsConsulting/django-plotly-dash

2. celery & redis asynchronous task execution:
   * https://rakibul.net/django-celery-1
   * https://medium.com/@markgituma/using-django-2-with-celery-and-redis-21343284827c
   * https://computingforgeeks.com/configure-celery-supervisord-centos-7-django-virtualenv/

3. django-inline-admin-extensions
   * Django admin interface page with tabularinline will become slow to load when the list of tabular items become too long (> 30 items). 
     The solution is to add pagination (with 20 items per page) to the tabularinline. After googling around, a working solution is found at:
     https://github.com/ctxis/django-inline-admin-extensions
