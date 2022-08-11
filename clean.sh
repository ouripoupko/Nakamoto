for i in {5000..5099}
do
  mongo Nakamoto_$i --eval 'db.dropDatabase()'
done
