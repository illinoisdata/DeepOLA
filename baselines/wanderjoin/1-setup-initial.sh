set -o xtrace

# Initial installs
sudo apt update
sudo apt -y install build-essential
sudo apt -y install bison
sudo apt -y install m4
sudo apt -y install libreadline-dev
sudo apt -y install zlib1g-dev

# Download Flex
if [ ! -d "flex-2.5.31" ]; then
	wget https://launchpad.net/ubuntu/+archive/primary/+sourcefiles/flex/2.5.31-31/flex_2.5.31.orig.tar.gz
	tar -xvf flex_2.5.31.orig.tar.gz
fi
cd flex-2.5.31
PATH=$PATH:/usr/local/m4/bin/
./configure --prefix=/usr/local/flex
sudo make
sudo make install
PATH=$PATH:/usr/local/flex/bin
version=$(flex --version)
echo "Obtained Flex Version: $version"
