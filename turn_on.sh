for cpu in $(seq 0 31); do
  echo 1 | sudo tee /sys/devices/system/cpu/cpu$cpu/online
done

