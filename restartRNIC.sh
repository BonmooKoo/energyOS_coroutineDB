sudo rmmod ib_uverbs mlx5_ib mlx5_core
sleep 1
sudo modprobe mlx5_core
sudo modprobe mlx5_ib
sudo modprobe ib_uverbs
echo 1 > /sys/class/infiniband/mlx5_0/device/reset
opensm
