cmake_minimum_required(VERSION 2.8.3)
project(aioros_master)

find_package(catkin REQUIRED)

catkin_python_setup()
catkin_package()
install(PROGRAMS scripts/aiorosmaster
        DESTINATION ${CATKIN_GLOBAL_BIN_DESTINATION})
