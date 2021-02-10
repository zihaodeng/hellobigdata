#java:8 是使用jdk版本
FROM java:8

#指定了临时文件目录为/tmp。
VOLUME /tmp

#添加jar到容器并重命名
ADD lotemall-webapi-0.0.1.jar lotmall-webapi.jar

#执行jar文件
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/lotmall-webapi.jar"]
