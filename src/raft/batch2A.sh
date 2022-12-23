num=0
success=0
while (( num < 20 ))
do
((num++))
go test -run 2A
success++
echo "success:$success/20" 
done