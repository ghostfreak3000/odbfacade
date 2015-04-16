package com.terasoft.terautils.odbfacade;

public class Apple {

	private final String color, age;

	public Apple() {
		super();
		this.color = "DefaultColor";
		this.age = "DefaultAge";
	}

	public Apple(String color, String age) {
		super();
		this.color = color;
		this.age = age;
	}

	public String getColor() {
		return color;
	}

	public String getAge() {
		return age;
	}
	
}
