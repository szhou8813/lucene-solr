/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package redfin.search.similarities;

import org.apache.lucene.search.similarities.Similarity;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.schema.SimilarityFactory;

/**
 * Factory for {@link RedfinSimilarity}
 * <p>
 * Parameters:
 * <ul>
 *   <li>shortDocPenalty(float): Controls to what degree document length normalizes tf values.
 *                  Valid value between 0 and 1.
 *                  0 = no penalty.
 *                  1 = maximum penalty for 1 doc length (near 0 score).
 *                  The default is <code>0.5</code>
 * </ul>
 * <p>
 * Optional settings:
 * <ul>
 *   <li>discountOverlaps (bool): Sets
 *       {@link RedfinSimilarity#setDiscountOverlaps(boolean)}</li>
 * </ul>
 * @lucene.experimental
 */
public class RedfinSimilarityFactory extends SimilarityFactory {
  private boolean discountOverlaps;
  private float shortDocPenalty;

  @Override
  public void init(SolrParams params) {
    super.init(params);
    discountOverlaps = params.getBool("discountOverlaps", false);
    shortDocPenalty = params.getFloat("shortDocPenalty", 0.5f);
  }

  @Override
  public Similarity getSimilarity() {
    RedfinSimilarity sim = new RedfinSimilarity(shortDocPenalty);
    sim.setDiscountOverlaps(discountOverlaps);
    return sim;
  }
}
