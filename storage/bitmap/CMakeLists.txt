# Copyright (c) 2006, 2010, Oracle and/or its affiliates. All rights reserved.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA


# Preliminaries -----------------------------------------------

add_definitions(-Wno-non-virtual-dtor -Wno-long-long)

# add_definitions(-save-temps) # No "<MACRO> redefined" warnings [typecode_expansion]

INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/sql)
IF(APPLE)
   SET(CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS
       "${CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS} -flat_namespace -undefined suppress")
#   TARGET_LINK_LIBRARIES(engine_tools sql mysys)
ENDIF(APPLE)


#    QSORT.h                  QSORT.cc

# ENGINE-TOOLS ------------------------------------------------
SET(ENGINE_TOOLS_SOURCES
    ha_bitmap.h              
    ha_scalarray.h
    ha_scalarray_toolbox-1x1.cc	ha_scalarray_toolbox-1x9.cc	
    ha_scalarray_toolbox-2x8.cc	ha_scalarray_toolbox-4x5.cc	
    ha_scalarray_toolbox-8x4.cc
    ha_scalarray_toolbox-1x2.cc	ha_scalarray_toolbox-2x1.cc	
    ha_scalarray_toolbox-2x9.cc	ha_scalarray_toolbox-4x8.cc	
    ha_scalarray_toolbox-8x5.cc
    ha_scalarray_toolbox-1x4.cc	ha_scalarray_toolbox-2x2.cc	
    ha_scalarray_toolbox-4x1.cc	ha_scalarray_toolbox-4x9.cc	
    ha_scalarray_toolbox-8x8.cc
    ha_scalarray_toolbox-1x5.cc	ha_scalarray_toolbox-2x4.cc	
    ha_scalarray_toolbox-4x2.cc	ha_scalarray_toolbox-8x1.cc	
    ha_scalarray_toolbox-8x9.cc
    ha_scalarray_toolbox-1x8.cc	ha_scalarray_toolbox-2x5.cc	
    ha_scalarray_toolbox-4x4.cc	ha_scalarray_toolbox-8x2.cc
    bitbucket.h              bitbucket.cc
    ha_handler.h             ha_handler.cc
    ha_scalarray_toolbox.h   ha_scalarray_toolbox.cc
    bucket.h                 bucket.cc
    logging.h                logging.cc
    scalarrbucket.h          scalarrbucket.cc
    )

ADD_LIBRARY(engine_tools SHARED ${ENGINE_TOOLS_SOURCES})

INSTALL(TARGETS engine_tools DESTINATION ${INSTALL_LIBDIR} COMPONENT Development)

SET(ENGINE_TOOLS_PLUGIN_DYNAMIC "engine_tools")
SET(ENGINE_TOOLS_PLUGIN_MANDATORY TRUE)

# BITMAP -------------------------------------------------------

SET(bitmap_sources ha_bitmap.cc ha_bitmap.h bitbucket.h bucket.h logging.h)
SET(scalarray_sources ha_scalarray.cc ha_scalarray.h ha_scalarray_toolbox.h 
		      scalarrbucket.h bitbucket.h bucket.h logging.h)
set(udf_example_sources udf_example.cc ha_scalarray.h ha_scalarray_toolbox.h 
		      scalarrbucket.h bitbucket.h bucket.h logging.h)

MYSQL_ADD_PLUGIN(bitmap ${bitmap_sources} STORAGE_ENGINE MANDATORY MODULE_ONLY)
MYSQL_ADD_PLUGIN(scalarray ${scalarray_sources} STORAGE_ENGINE MANDATORY MODULE_ONLY)
MYSQL_ADD_PLUGIN(udfs ${udf_example_sources} STORAGE_ENGINE MANDATORY MODULE_ONLY)

ADD_DEPENDENCIES(engine_tools GenError)

add_dependencies(bitmap scalarray)
add_dependencies(udfs scalarray bitmap engine_tools)

target_link_libraries(bitmap    engine_tools)
target_link_libraries(scalarray engine_tools)
target_link_libraries(udfs      engine_tools)

