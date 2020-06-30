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
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SmallFloat;

public class RedfinSimilarity extends Similarity {

  public RedfinSimilarity() {
  }

  /**
   * True if overlap tokens (tokens with a position of increment of zero) are
   * discounted from the document's length.
   */
  protected boolean discountOverlaps = true;

  /** Sets whether overlap tokens (Tokens with 0 position increment) are
   *  ignored when computing norm.  By default this is false, meaning overlap
   *  tokens do count when computing norms. */
  public void setDiscountOverlaps(boolean v) {
    discountOverlaps = v;
  }

  /**
   * Returns true if overlap tokens are discounted from the document's length.
   * @see #setDiscountOverlaps
   */
  public boolean getDiscountOverlaps() {
    return discountOverlaps;
  }

  /** Cache of decoded bytes. */
  private static final float[] LENGTH_TABLE = new float[256];

  static {
    for (int i = 0; i < 256; i++) {
      LENGTH_TABLE[i] = SmallFloat.byte4ToInt((byte) i);
    }
  }

  @Override
  public final long computeNorm(FieldInvertState state) {
    final int numTerms = discountOverlaps ? state.getLength() - state.getNumOverlap() : state.getLength();
    return SmallFloat.intToByte4(numTerms);
  }

  @Override
  public final SimWeight computeWeight(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
    return new RedfinWeights(collectionStats.field(), boost);
  }

  @Override
  public final SimScorer simScorer(SimWeight weights, LeafReaderContext context) throws IOException {
    RedfinWeights redfinWeights = (RedfinWeights) weights;
    return new RedfinDocScorer(redfinWeights, context.reader().getNormValues(redfinWeights.field));
  }

  private class RedfinDocScorer extends SimScorer {
    private final RedfinWeights weights;
    private final NumericDocValues norms;
    private final float weightTotalValue; // for now just the boost itself

    RedfinDocScorer(RedfinWeights weights, NumericDocValues norms) throws IOException {
      this.weights = weights;
      this.weightTotalValue = weights.boost;
      this.norms = norms; // field length
    }

    @Override
    public float score(int doc, float freq) throws IOException {
      // if there are no norms, use freq itself instead
      // so freq / norm will yield 1, giving full score to the document
      float norm;
      if (norms == null) {
        norm = freq;
      } else {
        if (norms.advanceExact(doc)) {
          // use the predecoded table
          norm = LENGTH_TABLE[((byte) norms.longValue()) & 0xFF];
        } else {
          norm = freq;
        }
      }
      return weightTotalValue * freq / norm;
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
      return explainScore(doc, freqExpl, weights, norms);
    }
}

  /** Collection statistics for the Redfin model. */
  private static class RedfinWeights extends SimWeight {
    /** query boost */
    private final float boost;
    /** field name, for pulling norms */
    private final String field;

    RedfinWeights(String field, float boost) {
      this.field = field;
      this.boost = boost;
    }
  }

  private Explanation explainTFNorm(
      int doc,
      Explanation freqExpl,
      RedfinWeights weights,
      NumericDocValues norms,
      float[] lengthCache) throws IOException {
    if (norms == null) {
      return Explanation.match(1, "tfNorm, computed as 1 in absence of norms");
    }

    float doclen;
    if (norms.advanceExact(doc)) {
      doclen = lengthCache[(byte) norms.longValue()];
    } else {
      doclen = freqExpl.getValue();
    }
    return Explanation.match(
        (freqExpl.getValue() / doclen),
        "tfNorm, computed as (freq / fieldLength) with freq=" + freqExpl.getValue() + ", fieldLength=" + doclen);
  }

  private Explanation explainScore(int doc, Explanation freqExpl, RedfinWeights weights, NumericDocValues norms) throws IOException {
    Explanation boostExpl = Explanation.match(weights.boost, "boost");
    List<Explanation> subs = new ArrayList<>();
    if (boostExpl.getValue() != 1.0f)
      subs.add(boostExpl);
    Explanation tfNormExpl = explainTFNorm(doc, freqExpl, weights, norms, LENGTH_TABLE);
    subs.add(tfNormExpl);
    return Explanation.match(
        boostExpl.getValue() * tfNormExpl.getValue(),
        "score(doc="+doc+",freq="+freqExpl+"), product of:", subs);
  }

  @Override
  public String toString() {
    return "RedfinSimilarity";
  }
}
