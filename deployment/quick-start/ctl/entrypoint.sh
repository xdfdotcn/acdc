#/bin/bash

api_server="http://api:9602"
tool_path="/acdc-ctl/"
schema_registry_url="http://schema_registry:8081"
connect_source_mysql_rest="http://connect_source_mysql:8083"
connect_sink_mysql_rest="http://connect_sink_mysql:8083"
kafka_server="kafka:9092"
kafka_host="kafka"
kafka_port="9092"
meta_db_host="meta_db"
meta_db_port="3306"
meta_db_user="root"
meta_db_password="root"
meta_db_database="acdc"

# parsing json
getJsonValuesByAwk() {
    awk -v json="$1" -v key="$2" -v defaultValue="$3" 'BEGIN{
        foundKeyCount = 0
        while (length(json) > 0) {
            # pos = index(json, "\""key"\"");
            pos = match(json, "\""key"\"[ \\t]*?:[ \\t]*");
            if (pos == 0) {if (foundKeyCount == 0) {print defaultValue;} exit 0;}

            ++foundKeyCount;
            start = 0; stop = 0; layer = 0;
            for (i = pos + length(key) + 1; i <= length(json); ++i) {
                lastChar = substr(json, i - 1, 1)
                currChar = substr(json, i, 1)

                if (start <= 0) {
                    if (lastChar == ":") {
                        start = currChar == " " ? i + 1: i;
                        if (currChar == "{" || currChar == "[") {
                            layer = 1;
                        }
                    }
                } else {
                    if (currChar == "{" || currChar == "[") {
                        ++layer;
                    }
                    if (currChar == "}" || currChar == "]") {
                        --layer;
                    }
                    if ((currChar == "," || currChar == "}" || currChar == "]") && layer <= 0) {
                        stop = currChar == "," ? i : i + 1 + layer;
                        break;
                    }
                }
            }

            if (start <= 0 || stop <= 0 || start > length(json) || stop > length(json) || start >= stop) {
                if (foundKeyCount == 0) {print defaultValue;} exit 0;
            } else {
                print substr(json, start, stop - start);
            }

            json = substr(json, stop + 1, length(json) - stop)
        }
    }'
}

cd "${tool_path}"

# waiting for login api system
token=""
until [ -n "$token" ]; do
  sh  acdc-api-user.sh --api-server ${api_server} --login --email admin --password admin > tmp.token
  tokenJson=`cat tmp.token | grep token`
  token=`getJsonValuesByAwk "$tokenJson" "token" ""| cat |sed 's/\"//g'|cat`
  echo "Wait for api server initialization to finish..."
  sleep 2
done

echo "Succeeded in logging in to the system, token is: ${token}"

# waiting for kafka
kafka_connected=""
until [ -n "$kafka_connected" ];do
  kafka_connected=`echo  -e  "\n"| telnet ${kafka_host} ${kafka_port} | grep Connected`
  sleep 2
    echo "Waiting for connect kafka ..."
done

echo "Connect to kafka ..."

# init data
source_mysql_initialized=""
until [ -n "$source_mysql_initialized" ];do
  source_mysql_initialized=` sh acdc-api-connect.sh --api-server ${api_server}  --token ${token} --create  --cluster-type SOURCE_MYSQL  --cluster-server ${connect_source_mysql_rest} --schema-registry-url ${schema_registry_url} --security-protocol PLAINTEXT | grep '\"code\":200\|\"code\":409'`
  sleep 2
  echo "Execute data initialization... result: ${source_mysql_initialized}"
done

sink_mysql_initialized=""
until [ -n "$sink_mysql_initialized" ];do
  sink_mysql_initialized=`sh acdc-api-connect.sh --api-server ${api_server}  --token ${token} --create  --cluster-type SINK_MYSQL  --cluster-server ${connect_sink_mysql_rest} --schema-registry-url ${schema_registry_url} --security-protocol PLAINTEXT | grep '\"code\":200\|\"code\":409'`
  sleep 2
  echo "Execute data initialization... result:  ${sink_mysql_initialized}"
done

inner_kafka_cluster_initialized=""
until [ -n "$inner_kafka_cluster_initialized" ];do
  inner_kafka_cluster_initialized=`sh acdc-api-kafka-cluster.sh --api-server ${api_server} --token ${token} --create --bootstrap-server ${kafka_server} --cluster-type INNER --security-protocol PLAINTEXT | grep '\"code\":200\|\"code\":409' `
  sleep 2
  echo "Execute data initialization... result:  ${inner_kafka_cluster_initialized}"
done

echo "Data initialization is complete..."
