#!/bin/bash
#-------------------------------------------------------------
#
# (C) Copyright IBM Corp. 2010, 2015
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#-------------------------------------------------------------
set -e

if [ "$4" == "SPARK" ]; then CMD="./sparkDML.sh "; DASH="-"; elif [ "$4" == "MR" ]; then CMD="hadoop jar SystemML.jar " ; else CMD="echo " ; fi

BASE=$3

export HADOOP_CLIENT_OPTS="-Xmx2048m -Xms2048m -Xmn256m"

echo "running decision tree"

#training
tstart=$SECONDS
${CMD} -f ../algorithms/decision-tree.dml $DASH-explain $DASH-stats $DASH-nvargs X=$1 Y=$2 fmt=csv M=${BASE}/M
ttrain=$(($SECONDS - $tstart - 3))
echo "DecisionTree train on "$1": "$ttrain >> times.txt

#predict
tstart=$SECONDS
${CMD} -f ../algorithms/decision-tree-predict.dml $DASH-explain $DASH-stats $DASH-nvargs M=${BASE}/M X=$1_test Y=$2_test P=${BASE}/P
tpredict=$(($SECONDS - $tstart - 3))
echo "DecisionTree predict on "$1": "$tpredict >> times.txt

