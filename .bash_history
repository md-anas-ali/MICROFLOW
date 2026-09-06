termux-setup-storage
cd storage/shared
pkg update -y && pkg upgrade -y
pkg install git unzip rsync -y
termux-setup-storage
ls ~/storage/downloads
cd ~/storage/shared
git clone https://github.com/md-anas-ali/MICROFLOW.git
cd MICROFLOW
git status
git config --global --add safe.directory /storage/emulated/0/MICROFLOW
git status
cd ~
rm -rf ~/microflow-update
mkdir ~/microflow-update
unzip ~/storage/downloads/MICROFLOW-main-FIXED.zip -d ~/microflow-update
cd ~/microflow-update
if [ $(ls -A | wc -l) = 1 ] && [ -d "$(ls -A)" ]; then cd "$(ls -A)"; fi
rsync -a --delete --exclude='.git' ./ ~/storage/shared/MICROFLOW/
cd ~/storage/shared/MICROFLOW
rm -rf ~/microflow-update
git status
pkg update -y && pkg upgrade -y
pkg install git unzip rsync -y
termux-setup-storage
git config --global user.name "md-anas-ali"
git config --global user.email "anas015987@gmail.com"
cd storage/shared
git clone https://github.com/md-anas-ali/MICROFLOW.git
cd MICROFLOW
cd ~
unzip /storage/emulated/0/Download/MICROFLOW-trimmed.zip -d unzipped
cd unzipped
if [ $(ls -A | wc -l) = 1 ] && [ -d "$(ls -A)" ]; then cd "$(ls -A)"; fi
rsync -a --delete --exclude='.git' ./ ~/storage/shared/MICROFLOW/
cd ~/storage/shared/MICROFLOW
rm -rf ~/unzipped
git add -A
git commit -m "Update repo with trimmed low-RAM MicroFlow build"
git push origin main
cd ~/storage/shared
git clone https://github.com/md-anas-ali/MICROFLOW.git
cd MICROFLOW
cd ~/storage/shared/MICROFLOW
cd ~
unzip "/storage/emulated/0/Download/MICROFLOW-fixed (2).zip" -d unzipped
cd unzipped
if [ $(ls -A | wc -l) = 1 ] && [ -d "$(ls -A)" ]; then cd "$(ls -A)"; fi
rsync -a --delete --exclude='.git' ./ ~/storage/shared/MICROFLOW/
cd ~/storage/shared/MICROFLOW
git add -A
git commit -m "Fix 512MB OOM and memory retention"
git push origin main
