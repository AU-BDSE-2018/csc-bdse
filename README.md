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
Мы не до конца поняли, что конкретно значит персистентность, поэтому данные постгреса сохраняем в /tmp/postgres_data.
Очевидно, такой способ не до конца персистентный, т.к. после перезапуска контейнера ноды tmp будет очищено, но судя
по изначально данным интеграционным тестам, это и предполагалось.

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

Самое проблемное место -- это volume'ы.

Кажется логичным создавать класть данные базы данных куда-нибудь в /tmp/somefolder файловой системы ноды.
Во всяком случае, строчка `VOLUME /tmp` в Dockerfile контейнера для ноды в интеграционных тестах намекает на это,
как мне кажется. Тут есть проблема. `docker run -v /tmp/somefoler:/... postgres` создаст эту папку `/tmp/somefolder`
на хосте. Упс. Я очень много времени потратил, но не понял, как создать ее внутри контейнера ноды.

Ещё я думал создать volume (docker volume create ...) и класть туда все данные. Но тут тоже есть минусы. Во-первых,
этот volume, конечно же, так же будет жить на хосте и тогда не понятно, чем это лучше, чем сохранять все в /tmp/somefolder
хоста. Во-вторых, этому volume'у надо дать имя. Логично, что это имя базируется на имени ноды (собственно, это +- все,
чем наши ноды могут различать друг от друга на данном этапе). Но вот незадача, в тестах ноде всегда дается имя
"node-0". Более того, тесты за собой не чистят, поэтому повторый запуск тестов приводит к тому, что мы пользуется старым
volume'ом и тест типа getPrefixTest у нас падает при повторном запуске. В-третьих, если `/tmp` ещё хотя бы автоматически
чистится при перезапуске машины, то volume'ы живут, пока их явно не удалишь (не считая этой https://github.com/moby/moby/pull/19568
фичи, но мы ее заюзать не можем).

Можно было бы, конечно, прямо внутри тестов удалять все эти volume'ы и старые контейнеры, но это, во-первых, какое-то
жуткое нарушение принципов инкапсуляции (тестам надо знать имена всех контейнеров/volume'ов и т.д.),
а во-вторых, удалять это надо все до создания контейнеров. Но создание контейнеров
помеченно как `@ClassRule`. Читаем документацию к этой аннотации

```
    If there are multiple
    annotated {@link ClassRule}s on a class, they will be applied in an order
    that depends on your JVM's implementation of the reflection API, which is
    undefined, in general. However, Rules defined by fields will always be applied
    before Rules defined by methods.
```

И понимаем, что у нас нет нормального способа завести метод `clean` почистить все до создания контейнеров. Печаль.

В общем, в какой-то момент мне надоело пытаться исхитриться и сделать все красиво, поэтому я решил тупо не выставлять в
тестах дефолтное имя ноды и хранить все в /tmp/somefolder хоста (somefolder, понятно, зависит от имени ноды, чтобы
у нас не было проблем с запуском нескольких нод).

В итоге это все приводит к тому, что в тестах мы не можем юзать константное имя ноды, т.к. в данном случае придется
при каждом перезапуске ручками (да ещё и с правами рута) удалять данные из /tmp. Т.к. это совсем ужастно, в тестах
приходится так же генерировать случайное имя.

Я знаю, что это фиговое решение, но блин, я уже устал рыться в документациях и натыкаться на фэйлы и я уже скорее согласен
на -2 балла за практику, чем проводить ещё хоть минуту в поисках нормального решения.

Ещё одна проблема кода, которую можно даже довольно легко исправить -- это logging. Мы все выводит в System.out/System.err.
Конечно, надо пользоваться нормальными логерами, но, как вы понимаете, проблема та же (хотя в большинстве случаев у них, стоит отметить,
довольно адекватный и нормально документированный API).

Вот, на наш взгляд -- это остновные тонкие моменты (помимо кодстайла и архитектуры, конечно, но поля этого README слишком
малы, чтобы полнустью описать проблемы этих характеров) и детали работы нашей ноды.