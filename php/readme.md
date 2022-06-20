# php 

php version >= 7.4 
# composer

composer.json file

                {
                    "require": {
                        "krakjoe/pthreads-polyfill": "^1.1"
                    },
                    "autoload": {
                        "psr-4": {            
                            //"your namespace\\": "your folder/"
                            "app\\": "app/"
                        }
                    }
                }

ref composer usage
                

                https://getcomposer.org/doc/01-basic-usage.md

                https://getcomposer.org/download/


# run

                composer require krakjoe/pthreads-polyfill
                composer update

                php program.php


