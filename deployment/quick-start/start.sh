#/bin/bash

cat<<EOT
    _       ____ ____   ____
   / \     / ___|  _ \ / ___|
  / _ \   | |   | | | | |
 / ___ \  | |___| |_| | |___
/_/   \_\  \____|____/ \____|

EOT

cat<<EOT

初始化数据卷文件夹,并授予执行权限...

EOT
volume_base_dir="./volumes/"
for  service in connect kafka meta_db prometheus grafana schema_registry zookeeper
do
  volume_dir=$volume_base_dir$service
  if [ ! -d "$volume_dir" ]; then
        mkdir -p $volume_dir
  fi
done
chmod -R 777 $volume_base_dir

cat<<EOT

停止 ACDC 所有服务...

EOT
docker-compose down

cat<<EOT

启动 ACDC 服务...

EOT
docker-compose up -d --build

containers=(
acdc_meta_db:9601
acdc_api:9602
acdc_ui:9603
acdc_controller:9604
acdc_prometheus:9090
acdc_grafana:3000
acdc_zookeeper:22181
acdc_kafka:29092
acdc_schema_registry:28081
acdc_schema_registry_ui:28000
acdc_connect_source_mysql:9606
acdc_connect_sink_mysql:9607
)

acdc_service_start_check(){
  acdc_api_container_name="acdc_api"
  target_container_name=$1
  target_container_port=$2
  service_started=""
  echo "检测 ${target_container_name} ${target_container_port} 容器启动状态..."
  echo ""
  until [ -n "$service_started" ];do
  service_started=`echo  -e  '\n'| telnet 127.0.0.1 ${target_container_port} | grep Connected`
  sleep 2
  echo "${target_container_name} ${target_container_port} 容器启动完成..."
  done
}

for  container   in ${containers[@]}
do
  container_port=${container#*:}
  container_name=${container%:*}
  acdc_service_start_check "$container_name" "$container_port"
done

cat<<EOT

ACDC 启动完成...

EOT

cat<<EOT

欢迎使用ACDC: http://127.0.0.1:9603

初始用户: admin/admin

EOT
