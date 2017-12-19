package org.gz;

import java.util.ArrayList;
import java.util.Stack;

public class FilterQuotation {
	//递归逻辑
	private String filterRecursive(String tmpstrs, ArrayList<String> arr){		
		String res = tmpstrs;
		String strs = tmpstrs;
		boolean flag = false;
		Stack<Integer> stack = new Stack<Integer>();
		int step = 0;
		while (!flag){						
			flag = true;
			for (int i = step; i < strs.length(); i++){				
				char ch = strs.charAt(i);
				if ((ch == '“')||((ch == '‘'))||(ch == '\"')&&(stack.empty())){
//					System.out.println("插入左引号:" + i);
					stack.push(i);
				} 
				else if ((ch == '”')||((ch == '’'))||(ch == '\"')&&(!stack.empty())){				
//					System.out.println("判别右引号:" + i);
					int j = stack.pop();
					step = j;
					String tmp = strs.substring(j, i+1);
//					System.out.println("子串:" + tmp);
					int num = arr.size();
					if (tmp.charAt(tmp.length() - 2) == '。')
						res = res.replace(tmp, "锟斤拷羌墙" + num + "。");
					else res = res.replace(tmp, "锟斤拷羌墙" + num);
					arr.add(tmp);
//					System.out.println("res:" + res);
					if (!stack.empty()) {
//						System.out.println("restart");
						flag = false;
						strs = res;
						break;						
					}
				}
			}						
		}
		stack.clear();
		return res;
	}
	
	private String filterRecover(String res, ArrayList<String> arr){
//		System.out.println("start recover:" + res);
		String[] ss = res.split("[。]");
		for (int i = 0; i < ss.length; i++){
			if (ss[i].contains("锟斤拷羌墙")){				
				if (ss[i].contains("规定")) ss[i] = ""; else{
					int j = ss[i].indexOf("锟斤拷羌墙");
//					System.out.println("找到替换标识:" + j);				
					String tmp = arr.get((int)ss[i].charAt(j+5) - 48);
					String tm = ss[i].substring(j, j+6);
					ss[i] = ss[i].replace(tm, tmp);					
				}
//				System.out.println("ss = " + ss[i]);
				if (ss[i].contains("锟斤拷羌墙"))
					ss[i] = filterRecover(ss[i], arr);
			}
		}
		String r2 = "";
		for (int i = 0; i < ss.length; i++){
			if ((!ss[i].equals("")))
				r2 = r2 + ss[i] + "。";			
		}
		if (r2.length()>0) r2 = r2.substring(0, r2.length() - 1);
		ss = null;
		return r2;
	}
	
	public String filterQuotation(String strs, ArrayList<String> arr){
		String res = filterRecursive(strs, arr);
		String r2 = filterRecover(res, arr);
		return r2;
	}
	
	public String filterQuotation2(String strs, ArrayList<String> arr){
		String res = filterRecursive(strs, arr);
		String r2 = filterRecover(res, arr);
		return r2;
	}
	
	public static void main(String[] args) {		
		String strs = "“12‘规’规定‘123。’。12”第二行\"123321\"。规定“去去去”。本院认为";
		String str2 = "马“牌轿车1辆（价值10万元），无现金、存款、债权，债务。在婚后的家庭生活中双方因家庭琐事产生矛盾，2015年7月28日双方分居生活至今。\n另查明，黑某的妹妹因购房资金不够，黑某向马某某借款3万元，还款期限为2012年10月1日。\n原审法院认为，根据《中华人民共和国婚姻法》第三十二条第三款第五项有下列情形之一,调解无效的，应准予离婚：……(五)其他导致夫妻感情破裂的情形；……”的规定";
		String str3 = "马“五其他”的规定";
		System.out.println(new FilterQuotation().filterQuotation(str2, new ArrayList<String>()));
	}
}
