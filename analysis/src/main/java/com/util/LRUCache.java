package com.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author AnLuyao
 * @date 2018-06-05 10:39
 */
public class LRUCache extends LinkedHashMap<String, Integer> {
    private static final long serialVersionUID = 1L;
    protected int maxElements;

    public LRUCache(int maxSize) {
        super(maxSize, 0.75F, true);
        this.maxElements = maxSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
        return this.size() > this.maxElements;
    }
}
