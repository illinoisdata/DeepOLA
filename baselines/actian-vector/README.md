# Baseline: Actian Vector

## Quick Instructions/Tips
1. Download Actian Vector 6.2 DEB and Actian Vector ASC Key File.
2. Install the downloaded key file:  `sudo apt add key <>`
3. Install the downloaded deb: `sudo apt install *.deb`
4. Once Actian Vector is installed, switch to `actian` user.
5. Run `source ~/.iniVWsh` to enable commands like `sql, vwload` etc.
6. Run `createdb <dbname>` to create the database.
7. Run `sql <dbname> < file.sql` to run the sql in file `file.sql` on the database `<dbname>`
8. Use `vwload` to load files in the database tables.
9. Run `optimizedb` to generate statistics after loading all the data.
10. Note: On loading 100GB `lineitem` , one might receive out-of-memory error. Need to modify `max-memory-size` in `vectorwise.conf`  in `/opt/Actian/Vector/data/`
11. Also, make sure that the directory corresponding to database resides on the SSD. Can ensure this by making a symlink of the directory to `/mnt` storage path.
