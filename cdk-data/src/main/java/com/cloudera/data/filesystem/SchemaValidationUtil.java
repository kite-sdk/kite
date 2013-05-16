package com.cloudera.data.filesystem;

import java.io.IOException;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.apache.avro.io.parsing.Symbol;

// TODO: replace with AVRO-1315 when generally available
class SchemaValidationUtil {

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
        return hasErrors(((Symbol.Alternative) symbol).symbols);
      case EXPLICIT_ACTION:
        return false;
      case IMPLICIT_ACTION:
        return symbol instanceof Symbol.ErrorAction;
      case REPEATER:
        Symbol.Repeater r = (Symbol.Repeater) symbol;
        return hasErrors(r.end) || hasErrors(r.production);
      case ROOT:
        return hasErrors(Arrays.copyOfRange(symbol.production, 1, symbol.production.length));
      case SEQUENCE:
        return hasErrors(symbol.production);
      case TERMINAL:
        return false;
      default:
        throw new RuntimeException("unknown symbol kind: " + symbol.kind);
    }
  }

  private static boolean hasErrors(Symbol[] symbols) {
    if(null != symbols) {
      for(Symbol s: symbols) {
        if (hasErrors(s)) {
          return true;
        }
      }
    }
    return false;
  }
}
