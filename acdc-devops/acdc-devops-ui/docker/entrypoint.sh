#!/bin/sh

# replace nginx config's placeholder by environment
envsubst '$ACDC_API_URL,$LISTEN_PORT' < /etc/nginx/conf.d/nginx_conf.temp > /etc/nginx/conf.d/default.conf
exec nginx -g 'daemon off;'
