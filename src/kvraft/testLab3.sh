truncate --size 0 testLab3.log
count=0
while (( count < 1000 )); do
  # shellcheck disable=SC2129
  echo "[Test $((count+1))]" >> testLab3.log
  go test >> testLab3.log
  echo "" >> testLab3.log
  (( count++ ))
done
passCount=$(grep -c "PASS" testLab3.log)
failCount=$(grep -c "FAIL" testLab3.log)
# shellcheck disable=SC2129
echo "[Result]" >> testLab3.log
echo "PASS: $passCount" >> testLab3.log
echo "FAIL: $failCount" >> testLab3.log