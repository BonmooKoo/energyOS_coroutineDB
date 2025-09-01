#must be sudo

PERF_LOG="./tmp/perf_log.csv"
BMC_LOG="./tmp/bmc_log.csv"
MERGED_LOG="./tmp/power_log.csv"

rm -f $PERF_LOG $BMC_LOG $MERGED_LOG

if perf list | grep -q energy-ram; then
        DRAM_EVENT="energy-ram"
else
        DRAM_EVENT="energy-dram"
fi

#echo "[*] Using DRAM event: $DRAM_EVENT"
# 1. perf 실행 (백그라운드)
PERF_START=$(date +%s)
export PERF_START
perf stat -I 1000 -a -e energy-pkg,energy-ram -x, > $PERF_LOG 2>&1 &
PERF_PID=$!
echo "[*] perf 시작됨 (PID: $PERF_PID, time $(date +%s)"

# 2. ipmitool 측정 (1초 루프, 백그라운드)
(
while true; do
        #perf stat -a -e energy-pkg,$DRAM_EVENT -x, sleep 1 2>> $PERF_LOG
        #BMC_OUTPUT=$(ipmitool dcmi power reading | awk '/Instantaneous/ {print $4}')
        #BMC_ENERGY=$(echo "BMC_OUTPUT" | awk '/Instantaneous/ {print $4}')
        #echo "$(date +%s),$BMC_ENERGY">>"$BMC_LOG"
        echo "$(date +%s),$(ipmitool dcmi power reading | awk '/Instantaneous/ {print $4}')" >> $BMC_LOG
        sleep 1
done
) &
BMC_PID=$!
echo "[*] ipmitool 루프 시작됨 (PID: $BMC_PID)"

sleep 15

#3. mcperf 실행 (main workload)
echo "[*] 벤치마크 실행 시작 $(date +%s)"
#sleep 10
#./mcperf -s localhost -t 10
#./db_bench --benchmarks fillrandom --db=./tmp/ --duration=30
"$@"
echo "[*] 벤치마크 종료됨"

sleep 10
# 4. 측정 프로세스 종료
kill -15 $PERF_PID
kill -15 $BMC_PID
echo "[*] perf와 ipmitool 종료됨"

# 5. Python 병합 처리
echo "[*] 로그 병합 중..."

python3 <<EOF
import pandas as pd
import os

# PERF 시작 시간 불러오기
PERF_START_EPOCH = float(os.environ["PERF_START"])

# perf 로그 읽기: elapsed time 기준
perf = pd.read_csv("$PERF_LOG", header=None, usecols=[0,1,3], names=["elapsed", "value", "event"])
perf["elapsed"] = perf["elapsed"].astype(float)

# 절대 timestamp 계산
perf["timestamp"] = PERF_START_EPOCH + perf["elapsed"]

# energy-pkg / energy-ram 두 줄씩 → 한 줄로 pivot
perf_pivot = perf.pivot_table(index="timestamp", columns="event", values="value").reset_index()

# BMC 로그 읽기
bmc = pd.read_csv("$BMC_LOG", names=["timestamp", "BMC_Watts"])
bmc["timestamp"] = bmc["timestamp"].astype(float)

# 병합
merged = pd.merge_asof(
    perf_pivot.sort_values("timestamp"),
    bmc.sort_values("timestamp"),
    on="timestamp",
    direction="nearest",
    tolerance=1  # seconds
)
# 컬럼 이름 정리
merged = merged.rename(columns={
    "energy-pkg": "pkg_value",
    "energy-ram": "ram_value",
    "BMC_Watts": "bmc_value"
})
merged.to_csv("$MERGED_LOG", index=False)
print("[*] 병합 완료 → 결과 저장: $MERGED_LOG")
EOF


kill -9 $PERF_PID
kill -9 $BMC_PID
