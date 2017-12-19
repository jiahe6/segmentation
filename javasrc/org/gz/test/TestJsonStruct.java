package org.gz.test;

public class TestJsonStruct {
	ResultClass result;

	public ResultClass getResult() {
		return result;
	}

	public void setResult(ResultClass result) {
		this.result = result;
	}
	
	@Override
	public String toString() {
		return(result.code + result.message);
	}
	
}

class ResultClass {
	String code;
	String message;
	public String getCode() {
		return code;
	}
	public void setCode(String code) {
		this.code = code;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	
}