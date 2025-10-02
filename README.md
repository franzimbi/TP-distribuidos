con ./run.sh se corre todo el sistema. client envia los archivos para recibir los 5 '.csv' resultado de las cuatro queries. 
los archivos son tomados de la carpera csvs_files  que se puede obtener de este kaggle: ( https://www.kaggle.com/datasets/geraldooizx/g-coffee-shop-transaction-202307-to-202506 )

para detener los conteiners se usa el script ./stop.sh


los tests se corren por separado con:
                                        - ./run_tests.sh exchange
                                        - ./run_tests.sh queues


el informe.pdf se encuentra en el repositorio mismo.
