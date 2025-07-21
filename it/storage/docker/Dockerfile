#
# Static HTTP server
#
FROM alpine:3.20
FROM nginx:latest

# Create web directory
WORKDIR /usr/share/nginx/html

# Download track files
RUN mkdir -p apm && \
    curl -O apm/span.json.bz2 "https://rally-tracks.elastic.co/apm/span.json.bz2"

# Expose nginx
EXPOSE 80

# Check for nginx stopcode
STOPSIGNAL SIGTERM

# Run nginx
CMD ["nginx", "-g", "daemon off;"]
