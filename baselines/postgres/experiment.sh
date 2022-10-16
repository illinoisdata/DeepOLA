set -o xtrace
/bin/bash 1-setup-postgres.sh 100 512M
/bin/bash 2-obtain-correct-output.sh 100
/bin/bash 2a-extract-output.sh 100
/bin/bash 3-time-queries.sh 100 1 10
python3 3a-extract-time.py 100 10
