/**
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
package org.kitesdk.data.spi;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.apache.avro.io.parsing.Symbol;

// TODO: replace with AVRO-1315 when generally available
public class SchemaValidationUtil {

  public static boolean canRead(Schema writtenWith, Schema readUsing) {
    try {
      return !hasErrors(new ResolvingGrammarGenerator().generate(
          writtenWith, readUsing));
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Returns true if the Parser contains any Error symbol, indicating that it may fail
   * for some inputs.
   */
  private static boolean hasErrors(Symbol symbol) {
    switch(symbol.kind) {
      case ALTERNATIVE:
        return hasErrors(symbol, ((Symbol.Alternative) symbol).symbols);
      case EXPLICIT_ACTION:
        return false;
      case IMPLICIT_ACTION:
        return symbol instanceof Symbol.ErrorAction;
      case REPEATER:
        Symbol.Repeater r = (Symbol.Repeater) symbol;
        return hasErrors(r.end) || hasErrors(symbol, r.production);
      case ROOT:
      case SEQUENCE:
        return hasErrors(symbol, symbol.production);
      case TERMINAL:
        return false;
      default:
        throw new RuntimeException("unknown symbol kind: " + symbol.kind);
    }
  }

  private static boolean hasErrors(Symbol root, Symbol[] symbols) {
    if(null != symbols) {
      for(Symbol s: symbols) {
        if (s == root) {
          continue;
        }
        if (hasErrors(s)) {
          return true;
        }
      }
    }
    return false;
  }
}
