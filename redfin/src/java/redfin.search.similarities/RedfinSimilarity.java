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


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;

public class RedfinSimilarity extends Similarity {

  public RedfinSimilarity() {
  }

  @Override
  public final long computeNorm(FieldInvertState state) {
    return 1L;
  }

  @Override
  public final SimWeight computeWeight(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
    return new RedfinWeights(boost);
  }

  @Override
  public final SimScorer simScorer(SimWeight weights, LeafReaderContext context) throws IOException {
    RedfinWeights redfinWeights = (RedfinWeights) weights;
    return new RedfinDocScorer(redfinWeights);
  }

  private class RedfinDocScorer extends SimScorer {
    private final RedfinWeights weights;
    private final float weightTotalValue; // for now just the boost itself

    RedfinDocScorer(RedfinWeights weights) throws IOException {
      this.weights = weights;
      this.weightTotalValue = weights.boost;
    }

    @Override
    public float score(int doc, float freq) throws IOException {
      // we rate with full score if match, and zero if no match.
      // this way the weight alone fully controls the value
      return weightTotalValue * (freq > 0 ? 1 : 0);
    }

    @Override
    public float computeSlopFactor(int distance) {
      return 1.0f;
    }

    @Override
    public float computePayloadFactor(int doc, int start, int end, BytesRef payload) {
      return 1.0f;
    }

    @Override
    public Explanation explain(int doc, Explanation freqExpl) throws IOException {
      return explainScore(doc, freqExpl, weights);
    }
}

  /** Collection statistics for the Redfin model. */
  private static class RedfinWeights extends SimWeight {
    /** query boost */
    private final float boost;

    RedfinWeights(float boost) {
      this.boost = boost;
    }
  }

  private Explanation explainScore(int doc, Explanation freqExpl, RedfinWeights weights) throws IOException {
    Explanation boostExpl = Explanation.match(weights.boost, "boost");
    List<Explanation> subs = new ArrayList<>();
    if (boostExpl.getValue() != 1.0f)
      subs.add(boostExpl);
    return Explanation.match(
        boostExpl.getValue(),
        "score(doc="+doc+",freq="+freqExpl+"), product of:", subs);
  }

  @Override
  public String toString() {
    return "RedfinSimilarity";
  }
}
