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

/**
 * Redfin Similarity specially designed for autocomplete
 * for region and addresses.
 *
 * This similarity doesn't impose IDF influence and implements
 * a much simplified version of TFNorm. The TFNorm assigns a
 * flat score as long as match is present, regardless of
 * term frequency, but applies penalty proportional to document length.
 *
 * See
 * https://docs.google.com/document/d/1jHGIjMBDS7rdqXLTCMAjejdzIIEitwZ6eYf1P_sfQ7c
 * @author Search Team
 * @author Seekers Team
 * @author Jason Zhou
 */
public class RedfinSimilarity extends Similarity {
  // between 0 and 1
  // 0 = no penalty
  // 1 = maximum penalty for 1 doc length (near 0 score)
  private final float shortDocPenalty;

  public RedfinSimilarity(float shortDocPenalty) {
    if (Float.isNaN(shortDocPenalty) || shortDocPenalty < 0 || shortDocPenalty > 1) {
      throw new IllegalArgumentException("illegal b value: " + shortDocPenalty + ", must be between 0 and 1");
    }
    this.shortDocPenalty = shortDocPenalty;
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
  private static final float[] OLD_LENGTH_TABLE = new float[256];
  private static final float[] LENGTH_TABLE = new float[256];

  static {
    for (int i = 1; i < 256; i++) {
      float f = SmallFloat.byte315ToFloat((byte)i);
      OLD_LENGTH_TABLE[i] = 1.0f / (f*f);
    }
    OLD_LENGTH_TABLE[0] = 1.0f / OLD_LENGTH_TABLE[255]; // otherwise inf

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
    float[] oldNormCache = new float[256];
    float[] normCache = new float[256];
    for (int i = 0; i < normCache.length; i++) {
      oldNormCache[i] = (1.0f - shortDocPenalty / OLD_LENGTH_TABLE[i]) / OLD_LENGTH_TABLE[i];
      normCache[i] = (1.0f - shortDocPenalty / LENGTH_TABLE[i]) / LENGTH_TABLE[i];
    }
    return new RedfinWeights(collectionStats.field(), boost, oldNormCache, normCache);
  }

  @Override
  public final SimScorer simScorer(SimWeight weights, LeafReaderContext context) throws IOException {
    RedfinWeights redfinWeights = (RedfinWeights) weights;
    return new RedfinDocScorer(redfinWeights, context.reader().getMetaData().getCreatedVersionMajor(), context.reader().getNormValues(redfinWeights.field));
  }

  private class RedfinDocScorer extends SimScorer {
    private final RedfinWeights weights;
    private final NumericDocValues norms;
    private final float weightTotalValue; // for now just the boost itself
    /** precomputed cache for all length values */
    private final float[] lengthCache;
    /** precomputed norm[256] with (1.0f - shortDocPenalty / docLen) / docLen; */
    private final float[] normCache;

    RedfinDocScorer(RedfinWeights weights, int indexCreatedVersionMajor, NumericDocValues norms) throws IOException {
      this.weights = weights;
      this.weightTotalValue = weights.boost;
      this.norms = norms; // field length
      if (indexCreatedVersionMajor >= 7) {
        lengthCache = LENGTH_TABLE;
        this.normCache = weights.normCache;
      } else {
        lengthCache = OLD_LENGTH_TABLE;
        this.normCache = weights.oldNormCache;
      }
    }

    @Override
    public float score(int doc, float freq) throws IOException {
      // For Redfin's specific use case, higher term frequency should not
      // contribute to more relevance. Use discrete value 0 or 1.
      float match = freq > 0 ? 1 : 0;
      // If there is no usable norm, use 1 instead to yield full score
      if (norms != null && norms.advanceExact(doc)) {
        // Use the predecoded table
        float norm = normCache[((byte) norms.longValue()) & 0xFF];
        return weightTotalValue * match * norm;
      }
      // When norm is not available, give full score
      return weightTotalValue * match;
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
      return explainScore(doc, freqExpl, weights, norms, lengthCache);
    }
}

  /** Collection statistics for the Redfin model. */
  private static class RedfinWeights extends SimWeight {
    /** query boost */
    private final float boost;
    /** field name, for pulling norms */
    private final String field;
    /** precomputed norm[256] with (1.0f - shortDocPenalty / docLen) / docLen; */
    private final float[] oldNormCache;
    private final float[] normCache;

    RedfinWeights(String field, float boost, float[] oldNormCache, float[] normCache) {
      this.field = field;
      this.boost = boost;
      this.oldNormCache = oldNormCache;
      this.normCache = normCache;
    }
  }

  private Explanation explainTFNorm(
      int doc,
      Explanation freqExpl,
      NumericDocValues norms,
      float[] lengthCache) throws IOException {
    float isMatch = freqExpl.getValue() > 0 ? 1 : 0;
    if (norms != null && norms.advanceExact(doc)) {
      float doclen = lengthCache[(byte) norms.longValue() & 0xFF];
      return Explanation.match(
          isMatch * (1 - shortDocPenalty/ doclen) / doclen,
          "tfNorm, computed as (isMatch * (1 - shortDocPenalty / fieldLength) / fieldLength) with isMatch=" + isMatch + ", fieldLength=" + doclen + ", shortDocPenalty=" + shortDocPenalty);
    }
    return Explanation.match(isMatch, "tfNorm, computed as isMatch * 1 in absence of usable norms");
  }

  private Explanation explainScore(int doc, Explanation freqExpl, RedfinWeights weights, NumericDocValues norms, float[] lengthCache) throws IOException {
    Explanation boostExpl = Explanation.match(weights.boost, "boost");
    List<Explanation> subs = new ArrayList<>();
    if (boostExpl.getValue() != 1.0f)
      subs.add(boostExpl);
    Explanation tfNormExpl = explainTFNorm(doc, freqExpl, norms, lengthCache);
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
