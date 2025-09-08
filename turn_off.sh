for cpu in $(seq 16 31); do
  echo 0 | sudo tee /sys/devices/system/cpu/cpu$cpu/online
done
