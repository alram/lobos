set -x
sudo ./src/spdk/scripts/setup.sh reset
sudo nvme format --ses=1 /dev/disk/by-id/nvme-WD_BLACK_SN7100_500GB_25333K801210_1 --force
sudo ./src/spdk/scripts/setup.sh
sudo ./src/spdk/scripts/setup.sh status
