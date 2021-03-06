/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.fuseinfo.ber.util;

import java.io.Serializable;

public class MathUtils implements Serializable{
	private static final long serialVersionUID = -8286494420420192011L;

	public static long murmurHash64A(final byte[] key, int len, final int seed) {
        final long m = 0xc6a4a7935bd1e995L;
        final int r = 47;
        int size = len - 7;
        long h = seed ^ len;
       
        //main loop
        for(int i = 0; i < size; i+=8){
            long k = ((long)key[i]) | (((long)key[i+1])<<8) | (((long)key[i+2])<<16) | (((long)key[i+3])<<24) |
                    (((long)key[i+4])<<32) | (((long)key[i+5])<<40) | (((long)key[i+6])<<48) | (((long)key[i+7])<<56);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        //remaining
        switch(len & 7) {
        case 7: h ^= ((long)key[--len]) << 48;
        case 6: h ^= ((long)key[--len]) << 40;
        case 5: h ^= ((long)key[--len]) << 32;
        case 4: h ^= ((long)key[--len]) << 24;
        case 3: h ^= ((long)key[--len]) << 16;
        case 2: h ^= ((long)key[--len]) << 8;
        case 1: h ^= ((long)key[--len]);
                h *= m;
        };

        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;
        return h;
    }

	public static long murmurHash64A(String key, final int seed) {
		byte[] data = key.getBytes();
		return murmurHash64A(data, data.length, seed);
	}
	
	public static long murmurHash64A(String key) {
		byte[] data = key.getBytes();
		return murmurHash64A(data, data.length, 0);
	}
}
