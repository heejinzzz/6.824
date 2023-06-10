truncate --size 0 testLab2.log
count=0
while (( count < 1000 )); do
  # shellcheck disable=SC2129
  echo "[Test $((count+1))]" >> testLab2.log
  go test >> testLab2.log
  echo "" >> testLab2.log
  (( count++ ))
done
passCount=$(grep -c "PASS" testLab2.log)
failCount=$(grep -c "FAIL" testLab2.log)
# shellcheck disable=SC2129
echo "[Result]" >> testLab2.log
echo "PASS: $passCount" >> testLab2.log
echo "FAIL: $failCount" >> testLab2.log