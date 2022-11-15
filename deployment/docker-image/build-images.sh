#/bin/bash

#todo 这个打包脚本未完成，思考下是否有必要一键 build 所有 docker image

# 根路径
root_path="$( cd $( dirname $0 );cd ../../ ;pwd )"

# 各项目的编译路径
ui_build_path="${root_path}/acdc-devops/acdc-devops-ui"
api_build_path="${root_path}/acdc-devops/acdc-devops-backend/acdc-devops-backend-api/target"
controller_build_path="${root_path}/acdc-devops/acdc-devops-backend/acdc-devops-backend-scheduler/target"

connect_build_path="${root_path}/acdc-connect"

# docker compose 镜像构建路径
dc_ui_path="$root_path/deployment/docker-compose/acdc/ui"
dc_api_path="$root_path/deployment/docker-compose/acdc/api"
dc_controller_path="$root_path/deployment/docker-compose/acdc/controller"
dc_connect_connector_path="$root_path/deployment/docker-compose/acdc/connect/connectors"

clean_docker_compose_deployment_file(){
rm -rf "$dc_connect_connector_path/*"
rm -rf ${dc_ui_path}/dist/
rm -rf ${dc_api_path}/app.jar
}

build_connect_component(){
    cur_component_path=$1
    cur_component_name="${cur_component_path##*/}"
    cd "${cur_component_path}"
    mvn clean
    mvn dependency:copy-dependencies -DoutputDirectory="${dc_connect_connector_path}/${cur_component_name}" -DincludeScope=runtime -DincludeTypes=jar
    mvn dependency:copy-dependencies -DoutputDirectory="${dc_connect_connector_path}/${cur_component_name}" -DincludeScope=system -DincludeTypes=jar
    mvn package -Dmaven.test.skip=true
    cp target/*.jar "${dc_connect_connector_path}/${cur_component_name}/"
}

# 清除 docker-compose 的部署文件
clean_docker_compose_deployment_file

# Java 项目编译
cd "${root_path}"
mvn clean package -DskipTests -Dcheckstyle.skip=true

# Ui 项目编译
cd "$ui_build_path"
yarn build:test

#  docker-compose部署物, Ui
cp -r  "$ui_build_path"/dist "${dc_ui_path}"

#  docker-compose部署物, Api
cp "${api_build_path}"/*.jar "${dc_api_path}"/app.jar

#  docker-compose部署物, Controller
cp "${controller_build_path}"/*.jar "${dc_controller_path}"/app.jar

#  docker-compose部署物, Connect
cd "${connect_build_path}"
for sub_path in "${connect_build_path}"/acdc-connect-connector/acdc* "${connect_build_path}"/acdc-connect-smt/acdc* ; do
  if [[ -d $sub_path ]]; then
    build_connect_component "${sub_path}"
  fi
done
