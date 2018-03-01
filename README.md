## csc-bdse
Базовый проект для практики по курсу "Программная инженерия больших данных".

## Задания
[Подготовка](INSTALL.md)
[Задание 1](TASK1.md)

## Команда
- Малышева Александра
- Бугакова Надежда
- Кравченко Дмитрий

## Решение

Мы используем PostgreSql в качестве СУБД. Его контейнер мы поднимаем из приложения (см. util.containers package).

Для запуска контейнера СУБД мы используем библиотеку docker-java. Для связи с СУБД -- hibernate (см. kv.db package и
hibernate_postgres.cfg.xml в resources).
Чтобы docker-java работал изнутри контейнера (в случае интерграционных тестов, например), мы поменяли Dockerfile
для контейнера ноды, чтобы поставить туда docker client. Так же пробрасываем туда сокет докера (/var/run/docker.sock).

Одно из проблемных мест (как минимум, оно очень некрасиво написано) -- это PostgresContainerManager#waitContainerInit.
Дело в том, что запуск самого контейнера postres'a еще не говорит о том, что postgres готов принимать подключения.
Чтобы в этот момент Hibernate (а точнее JDBC) не упал с ошибкой подключения, приходится в этом waitContainerInit ждать,
пока постгрес будет готов. В этом методе мы написали просто какое-то решение, которое нашли в интернете, которое мы
запускаем в шеле. Это, конечно, фигово, и надо делать это через docker-java, но его API довольно кривой и вообще за
окном наконец-то солнышко, хватит сидеть за компьютером (в общем да, нам было лень).

Для хранения данных базы данных мы используем docker'овские volume'ы.

Ещё одна проблема кода, которую можно даже довольно легко исправить -- это logging. Мы все выводит в System.out/System.err.
Конечно, надо пользоваться нормальными логерами, но, как вы понимаете, проблема та же (хотя в большинстве случаев у них, стоит отметить,
довольно адекватный и нормально документированный API).

Вот, на наш взгляд -- это остновные тонкие моменты (помимо кодстайла и архитектуры, конечно, но поля этого README слишком
малы, чтобы полнустью описать проблемы этих характеров) и детали работы нашей ноды.