/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.morphline.stdlib;

import java.util.Collection;
import java.util.Collections;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;

import com.typesafe.config.Config;

/**
 * Replaces all record field values for which all of the following conditions hold:
 * 
 * 1) the field name matches at least one of the given nameBlacklist predicates but none of the
 * given nameWhitelist predicates.
 * 
 * 2) the field value matches at least one of the given valueBlacklist predicates but none of the
 * given valueWhitelist predicates.
 * 
 * If the blacklist specification is absent it defaults to MATCH ALL. If the whitelist specification
 * is absent it defaults to MATCH NONE.
 */
public final class ReplaceValuesBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("replaceValues");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ReplaceValues(this, config, parent, child, context, false);
  }

}
