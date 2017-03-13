package com.asiainfo.hive;

import java.util.UUID;

import org.junit.Test;

import junit.framework.TestCase;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{

	@Test
	public void testTime(){
		System.out.println(System.currentTimeMillis());
		System.out.println(System.currentTimeMillis());
		System.out.println(System.nanoTime());
		System.out.println(System.nanoTime());
		System.out.println(System.nanoTime());
		System.out.println(UUID.randomUUID().getLeastSignificantBits());
		System.out.println(UUID.randomUUID().getMostSignificantBits());
	}
	
	
}
