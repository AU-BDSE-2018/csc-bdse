#Что нужно для старта разработки:
- Необходимо установить Java SE Development Kit 8 http://www.oracle.com/technetwork/java/javase/downloads/2133151  
- Для первоначальной сборки проекта необходим доступ до maven-репозиториев зависимостей (может быть неожиданно много,
 но скачиваются один раз)
- Необходимо установить Docker CE https://www.docker.com/community-edition
- Для первоначальной сборки контейнеров интеграционных тестов необходим доступ до Docker HUB (openjdk:8-jdk-alpine)
- На Linux необходимо добавить текущего пользователя в группу docker https://docs.docker.com/install/linux/linux-postinstall/
- Рекомендуется использовать IntelliJ IDEA https://www.jetbrains.com/idea/download/
- Не рекомендуется использовать Windows https://www.testcontainers.org/usage/windows_support.html 

#Структура модулей проекта
- bdse-app содержит код реализуемого бизнес-приложения 
- bdse-kvnode содержит код Persistent Storage Unit 
- bdse-integration-tests содержит утилиты и тесты для интеграционного тестирования

#Сборка и запуск интеграционных тестов
./mvnw --projects bdse-kvnode clean package
./mvnw --projects bdse-integration-tests --also-make test


#Детали запуска нашей ноды
Вообще, по-идее, ничего вам делать не надо -- просто запускаете Application.class или интеграционные тесты и все должно
работать (интеграционные тесты, конечно, можно запускать только после mvn package). 
Единтсвенное, на что мы опираемся, -- это что ваш user добавлен в группу docker'a. Также стоит учесть, что мы
всю разработку проводили под линуксом, поэтому точно не можем сказать, есть ли какие-то проблемы с маком/виндой.
