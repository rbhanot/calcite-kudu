/* Copyright 2021 Twilio, Inc
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
package com.twilio.kudu.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.linq4j.function.EqualityComparer;
import org.apache.calcite.linq4j.function.Predicate2;

public class KuduRowCache {
  private final LinkedList<CacheHit> cacheList;
  private final EqualityComparer<Object> comparer;
  private final int cacheSize;

  public KuduRowCache(final EqualityComparer<Object> comparer, final int cacheSize) {
    this.comparer = comparer;
    this.cacheList = new LinkedList<>();
    this.cacheSize = cacheSize;
  }

  public SearchResult cacheSearch(final List<Object> batchFromLeftTable, final Predicate2 joinCondition) {
    // @TODO: size estimation? We know it won't be larger than batchFromRight
    final ArrayList<Object> cacheMisses = new ArrayList<>();
    final ArrayList<Integer> indicesInCache = new ArrayList<>();

    final HashSet<CacheHit> hits = new HashSet<>();
    for (Object leftRow : batchFromLeftTable) {
      boolean foundInCache = false;
      for (int j = 0; j < cacheList.size(); j++) {
        final CacheHit cachedRightRow = cacheList.get(j);
        if (joinCondition.apply(leftRow, cachedRightRow.rowValue) && hits.add(cachedRightRow)) {
          indicesInCache.add(j);
          foundInCache = true;
          break;
        }
      }
      if (!foundInCache) {
        cacheMisses.add(leftRow);
      }
    }

    // shuffle the linked list
    // Remove the hits from the list
    indicesInCache.stream().forEachOrdered(idx -> cacheList.remove(idx));

    // Place the hits at the front
    hits.stream().forEach(hit -> cacheList.push(hit));

    if (hits.isEmpty()) {
      return new SearchResult(Collections.emptyList(), cacheMisses);
    } else {
      return new SearchResult(hits.stream().map(h -> h.rowValue).collect(Collectors.toList()), cacheMisses);
    }
  }

  public void addToCache(final Object rightRow) {
    cacheList.push(new CacheHit(rightRow));
    if (cacheList.size() > cacheSize) {
      cacheList.pollLast();
    }
  }

  private class CacheHit {
    final Object rowValue;

    CacheHit(Object hit) {
      this.rowValue = hit;
    }

    @Override
    public boolean equals(final Object o) {
      if (o instanceof CacheHit) {
        return comparer.equal(rowValue, ((CacheHit) o).rowValue);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return comparer.hashCode(rowValue);
    }
  }

  public class SearchResult {
    /**
     * Rows from the in memory cache that match rows on the left.
     */
    List<Object> cachedRightRows;

    /**
     * Rows from the left table that do not match anything in the cache;
     */
    List<Object> cacheMisses;

    SearchResult(final List<Object> cachedRightRows, final List<Object> cacheMisses) {
      this.cachedRightRows = cachedRightRows;
      this.cacheMisses = cacheMisses;
    }
  }
}
