FROM nginx:stable-alpine

# You need overwrite next two environments by docker run --env.
ENV ACDC_API_URL api.acdc.io/
ENV LISTEN_PORT 80

ADD dist/ /usr/share/nginx/html/
ADD docker/nginx_conf.temp /etc/nginx/conf.d/nginx_conf.temp
ADD docker/entrypoint.sh /entrypoint.sh

ENTRYPOINT sh /entrypoint.sh
