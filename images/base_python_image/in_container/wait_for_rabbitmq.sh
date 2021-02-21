while true
do
  check_result=$(curl -f rabbitmq:15672)
  result_status_code=$?

  if [[ ${result_status_code} == 0 ]]; then
    break
  fi

  sleep 3
done



