#!/bin/bash
source /etc/profile
package_path=$(pwd)
rm -rf ${package_path}/target/connector-package/*
for project_path in "$@"; do
  echo "current package project path:"$project_path
  cd $project_path
  project_name="${project_path##*/}"
  mvn clean
  mvn dependency:copy-dependencies -DoutputDirectory=${package_path}/target/connector-package/${project_name} -DincludeScope=runtime -DincludeTypes=jar
  mvn dependency:copy-dependencies -DoutputDirectory=${package_path}/target/connector-package/${project_name} -DincludeScope=system -DincludeTypes=jar
  mvn package -Dmaven.test.skip=true
  cp target/*.jar ${package_path}/target/connector-package/${project_name}/
done
