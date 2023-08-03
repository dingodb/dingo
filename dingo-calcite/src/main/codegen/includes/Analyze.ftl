<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->


SqlAnalyze SqlAnalyze(): {
  final Span s;
  final SqlAnalyze sqlAnalyze;
  final SqlIdentifier tableId;
  SqlIdentifier colId;
  Number numberTmp;
  Integer buckets = 0;
  Number cmSketchHeight = null;
  Number cmSketchWidth = null;
  Long samples = 0l;
  Float sampleRate = 0f;
  List<SqlIdentifier> colIds = new ArrayList<SqlIdentifier>();
} {
  <ANALYZE> { s = span(); }
  <TABLE> tableId = CompoundIdentifier()
  [ <COLUMNS> colId = SimpleIdentifier() { colIds.add(colId); }
    (
     <COMMA>
     colId = SimpleIdentifier() { colIds.add(colId); }
    )*
  ]
  [
    <WITH> numberTmp = number()
     <BUCKETS> { buckets = numberTmp.intValue(); }
  ]
  [
     <WITH>
     <CMSKETCH> cmSketchHeight = number() cmSketchWidth = number()
  ]
  [
     <WITH> numberTmp = number()
     (
      <SAMPLES> { samples = numberTmp.longValue(); }
      |
      <SAMPLERATE> { sampleRate = numberTmp.floatValue(); }
     )
  ]
  {
    return new SqlAnalyze(s.end(this), tableId, colIds, cmSketchHeight, cmSketchWidth, buckets, samples, sampleRate );
  }
}
