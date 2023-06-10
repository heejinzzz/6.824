truncate --size 0 testLab4A.log
count=0
while (( count < 1000 )); do
  # shellcheck disable=SC2129
  echo "[Test $((count+1))]" >> testLab4A.log
  go test >> testLab4A.log
  echo "" >> testLab4A.log
  (( count++ ))
done
passCount=$(grep -c "PASS" testLab4A.log)
failCount=$(grep -c "FAIL" testLab4A.log)
# shellcheck disable=SC2129
echo "[Result]" >> testLab4A.log
echo "PASS: $passCount" >> testLab4A.log
echo "FAIL: $failCount" >> testLab4A.log