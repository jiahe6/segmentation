package org.gz.test

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.sys.process._
import org.gz.ImportOrigin
import scala.io.Source
import java.io.File
import org.gz.data.importwenshu.ScheduleImport
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.apache.commons.compress.archivers.zip.ZipFile
import org.bson.Document
import tp.file.label.FindLabelByMongo
import main.DocHandlerMongoToMongo
import org.gz.data.segsecond.SegWithOrigin2
import test.TestDataGen1

object TestOthers {
  
  def testBasicLable = {
    val doc=new Document
		doc.append("_id", "a54e4b22-9dc1-41db-84b6-27628f864cf9");
		doc.append("path", "/home/cloud/wenshupart/20141231/甘肃/3205/民事案件/（2014）庆城民初字第833号_a54e4b22-9dc1-41db-84b6-27628f864cf9判决书.txt");
		doc.append("content", "张有文与俄克学、姚承伟民间借贷纠纷一审民事判决书\n甘肃省庆城县人民法院\n民 事 判 决 书\n（2014）庆城民初字第833号\n原告张有文。\n委托代理人吕民国，甘肃陇凤律师事务所律师。\n被告俄克学。\n被告姚承伟。\n原告张有文与被告俄克学、姚承伟民间借贷纠纷一案，本院于2014年5月22日立案受理后，依法组成合议庭公开开庭进行了审理。原告的委托代理人吕民国及被告姚承伟均到庭参加了诉讼，被告俄克学经本院合法传唤拒不到庭。本案现已审理终结。\n原告张有文诉称，2012年7月20日，被告俄克学作为借款人，姚承伟作为担保人与其签订借据及《个人借款／保证担保合同》，向其借款50万元用于工程周转，借款期限为两个月，即从2012年7月20日至9月20日止，每月借款到期前3日支付借款利息，月利率为20‰，如逾期未能归还借款，按日支付违约金20‰。借款期限届满后，被告俄克学未能归还借款，其向二被告多次催要，至今仍未归还。故诉请，1、依法判令被告俄克学、姚承伟连带归还其借款本金50万元及利息；2诉讼费由被告承担。\n被告俄克学未答辩。\n被告姚承伟辩称，其与原告张有文系朋友关系，因俄克学向张有文借款，张有文让其作为证明人在合同上签字，其并非担保人，即使其作为担保人，承担担保义务只有两个月，现已超过担保期限，故其不再承担担保义务。\n经审理查明，2012年7月20日，被告俄克学因工程资金周转需要，向原告张有文借款50万元，并给原告出具了借据一张，内容为“今借到张有文人民币50万元整，借款期限自2012年7月20日到2012年9月20日止，共两个月，全部借款于2012年9月20日前一次偿还；其中每月借款到期前3日归还借款利息，利息20‰。”同时姚承伟作为担保人，在该借据上签名，并承诺“作为保证人，在借款到期后如借款人无能力偿还借款本息费用时，我将以实物或现金形式担保借款本息、费用的归还及违约和出借人实现债权的一切费用。”2012年7月20日被告姚承伟给原告张有文出具了一份《保证承诺书》，同日原告张有文与被告俄克学、姚承伟签订了《个人借款、保证担保合同》，其中第十一条保证期间约定：“保证期间为本合同项下借款到期之日二年；出借人依据合同约定宣布借款提前到期的，保证期间为借款提前到期之日起两年。如果借款展期后，保证人继续承担保证责任，保证期限延至借款到期日之后二年。”合同签订后原告张有文向被告俄克学出借50万元用于工程周转，借款期限为两个月，月利率为20‰。借款期限届满后，被告俄克学未能归还借款，原告向二被告多次催要，二被告未归还借款，2013年3月19日被告姚承伟向原告再次出具一份保证书，内容为“俄克学借张有文现金50万元，我是保证人，我保证30天找到俄克学还现金，如果俄克学不还此款，我以我所有的财产归还张有文现金。保证人：姚承伟，2013年3月19日。”\n另查明，2012年7月6日至今中国人民银行6个月内贷款基准年利率为5.6%。\n上述事实，有当事人当庭陈述及当庭出示的下列证据证实，本院予以确认。\n1、借据一份，证实被告俄克学向原告借款50万元，利息为20‰，期限为两个月，保证人为姚承伟；\n2、个人借款、保证担保合同一份，证实姚承伟保证期限为借款到期之日起两年，保证范围为借款本息、罚息、违约金及实现债权的费用；\n3、保证书一份，证实姚承伟承诺，如俄克学不归还借款50万元，由其归还本息。\n本院认为，原告张有文与被告俄克学之间债权债务关系明确，被告俄克学与原告张有文签订了借款合同并出具了欠条，证据充分，被告俄克学应当按照双方约定的期限及时偿还借款本金50万元。原、被告在借款时约定月利率为20‰，最高人民法院《关于人民法院审理借贷案件的若干意见》第6条规定“民间借贷的利率可以适当高于银行的利率，各地人民法院可根据本地区的实际情况具体掌握，但最高不得超过银行同类贷款利率的四倍（包含利率本数）。超出此限度的，超出部分的利息不予保护。”因此原、被告之间所约定的利率未超过中国人民银行同类贷款利率的四倍，对原、被告约定的月利率20‰应予以支持。被告姚承伟作为连带保证人，连带保证人在主合同规定的债务履行期满没有履行债务的，债权人可以要求保证人在其保证范围内承担保证责任。被告姚承伟与原告张有文签订的《个人借款、保证担保合同》第十一条约定，姚承伟在欠款到期之日起两年内承担保证责任，原告张有文与被告俄克学是在2012年7月20日签订的借款合同，期限为两个月，原告在起诉时被告姚承伟仍在保证期内，因此原告请求被告姚承伟承担连带责任，理由正当，应予支持。依据《中华人民共和国合同法》第六十条、第二百一十条、第二百零五条、第二百零六条、第二百零七条、第二百一十一条、《中华人民共和国担保法》第七条、第十八条、《关于人民法院审理借贷案件的若干意见》第6条之规定，判决如下：\n被告俄克学于判决生效后三十日内归还原告张有文借款本金50万元，并支付自2012年7月21日起至该笔借款还清之日的利息（上述利息按原告张有文与被告俄克学约定的月利率20‰计算），被告姚承伟对上述借款本金及利息承担连带责任。\n如果未按本判决指定的期间履行给付金钱义务，应当依照《中华人民共和国民事诉讼法》第二百五十三条之规定，加倍支付迟延履行期间的债务利息。\n案件受理费8800元，由被告俄克学、姚承伟共同承担。\n如不服本判决，可在判决书送达之日起十五日内，向本院递交上诉状，并按对方当事人的人数提出副本，上诉于甘肃省庆阳市中级人民法院。\n本判决生效后，当事人必须按照判决所规定的履行期限自觉履行义务。依据《中华人民共和国民事诉讼法》第二百三十九条之规定，一方不履行的，对方应在判决书规定的履行期限届满之日起，二年内向本院申请强制执行。逾期视为放弃权利，法院将不再受理。\n审　判　长　　李广赟\n审　判　员　　魏鸿权\n人民陪审员　　米亚荣\n二〇一四年十月十七日\n书　记　员　　王艳锋");
		val new_doc=FindLabelByMongo.findBasicLabel(doc);
		new_doc
  }
  
  def getii = {
    val doc = testBasicLable
 	  val seg = new DocHandlerMongoToMongo
    val segdoc = seg.genSegData(doc)
 	  println(segdoc)
    var basiclabel = doc.get("basiclabel", classOf[Document])
    if (basiclabel != null) {    	
    	val lsls = segdoc.get("basiclabel.律师律所", classOf[Document])
    	if (lsls != null) (basiclabel.append("律师律所", lsls))
    	val spry = segdoc.get("basiclabel.审判人员", classOf[Document])
    	if (spry != null) (basiclabel.append("审判人员", spry))
    	val dsr = segdoc.get("basiclabel.当事人", classOf[Document])
    	if (dsr != null) (basiclabel.append("当事人", dsr))
    }
    val segdata = segdoc.get("segdata", classOf[Document])
    doc.append("basiclabel", basiclabel)
    if (segdata != null) (doc.append("segdata", segdata))
    println(doc)
  }
  
  def main(args: Array[String]): Unit = {
 	  	getii
  }
  
}
