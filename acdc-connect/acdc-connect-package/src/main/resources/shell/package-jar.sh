#!/bin/bash

source /etc/profile

package_path=$(pwd)
rm -rf ${package_path}/target/connector-package/*

maven_profile=$1
echo "current maven profile:"$maven_profile
shift 1

for project_path in "$@"; do
  echo "current package project path:"$project_path
  cd $project_path
  project_name="${project_path##*/}"
  mvn clean -P$maven_profile
  result_path=${package_path}/target/connector-package/${project_name}
  mkdir -p $result_path
  mvn dependency:copy-dependencies -DoutputDirectory=$result_path -DincludeScope=runtime -DincludeTypes=jar -P$maven_profile
  mvn dependency:copy-dependencies -DoutputDirectory=$result_path -DincludeScope=system -DincludeTypes=jar -P$maven_profile
  mvn package -Dmaven.test.skip=true -P$maven_profile
  cp target/*.jar $result_path
done
