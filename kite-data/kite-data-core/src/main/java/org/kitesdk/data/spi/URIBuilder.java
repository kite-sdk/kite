package org.kitesdk.data.spi;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.kitesdk.data.Dataset;

import com.google.common.collect.Maps;

/**
 * Builds dataset and view URIs 
 */
public class URIBuilder {
  private URI repoUri;
  private String datasetName;
  // LinkedHashMap preserves the order so that constructed URIs are more predictable
  private Map<String, String> equalityConstraints = Maps.newLinkedHashMap(); 

  public URIBuilder(String repoUri, String datasetName) {
    this(URI.create(repoUri), datasetName);
  }
  
  public URIBuilder(URI repoUri, String datasetName) {
    this.repoUri = repoUri;
    this.datasetName = datasetName;
  }
  
  /**
   * Adds a view constraint equivalent to {@link Dataset#with(String, Object)}
   * 
   * @param name the field name of the Entity
   * @param value the field value
   * @return this builder
   */
  public URIBuilder with(String name, Object value) {
    equalityConstraints.put(name, Conversions.makeString(value));
    return this;
  }
  
  /**
   * Returns the URI encompassing the given constraints.
   * 
   * @return the URI
   */
  public URI build() {
    URI repoStorageUri = URI.create(repoUri.getRawSchemeSpecificPart());
    Pair<URIPattern, Map<String, String>> pair =
        Registration.lookupPatternByRepoUri(repoStorageUri);
    Map<String, String> uriData = pair.second();
    uriData.put("dataset", datasetName);
    uriData.putAll(equalityConstraints);
    try {
      return new URI(
          equalityConstraints.isEmpty() ? "dataset" : "view",
          pair.first().construct(uriData).toString(),
          null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Could not build URI", e);
    }
  }
}