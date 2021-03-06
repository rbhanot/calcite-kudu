/* Copyright 2020 Twilio, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twilio.kudu.sql.rules;

import com.twilio.kudu.sql.KuduRelNode;
import com.twilio.kudu.sql.KuduWrite;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

import java.util.Arrays;

public class KuduWriteRule extends ConverterRule {
  public static final KuduWriteRule INSTANCE = new KuduWriteRule();

  private KuduWriteRule() {
    super(KuduWrite.class, KuduRelNode.CONVENTION, KuduRelNode.CONVENTION, "KuduWriteRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    final RelNode convertedInput = convert(rel.getInput(0), rel.getTraitSet());
    return rel.copy(rel.getTraitSet(), Arrays.asList(convertedInput));
  }
}