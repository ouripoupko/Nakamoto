
# python server.py 5000 {5000..5099} &

for i in {5000..5099}
do
  python server.py $i {5000..5099} > output/output_$i &
done

sleep 10

python client.py

sleep 10

ps -ef | grep "server.py" | grep -v grep | awk '{print $2}' | xargs kill
