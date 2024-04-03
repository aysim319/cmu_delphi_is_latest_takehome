# Delphi Is Latest Take-home

## If you're using Python, follow this guide

Open up the file data_fix.py. You will be modifying the functions in this file in order to output a correct resulting file. Feel free to use out function stubs or delete them and write your own! They are there to be helpful and give you a starting place, but they are not necessary to use.

## If you're using SQL, follow this guide

Start the database with the following command:

    docker run --rm -p 3306:3306 -e MYSQL_ROOT_PASSWORD=strong_password -e MYSQL_DATABASE=delphi -e MYSQL_USER=foo -e MYSQL_PASSWORD=bar -v $(pwd)/database/init/:/run/init -v $(pwd)/data/:/run/init/data -v $(pwd)/database/my.cnf:/etc/mysql/my.cnf --name delphi_db mysql:latest

In a seperate terminal, run:

    docker exec delphi_db /bin/sh -c 'chmod +x /run/init/initialize_database.sh && ./run/init/initialize_database.sh'

Lastly, run this command to get into the MySQL CLI:

    docker exec -it delphi_db mysql --user=foo --password=bar