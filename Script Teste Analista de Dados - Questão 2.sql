CREATE DATABASE  IF NOT EXISTS `teste_analista_dados_questao_2` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `teste_analista_dados_questao_2`;
-- MySQL dump 10.13  Distrib 8.0.26, for Win64 (x86_64)
--
-- Host: localhost    Database: teste_analista_dados_questao_2
-- ------------------------------------------------------
-- Server version	8.0.21

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `depto`
--

DROP TABLE IF EXISTS `depto`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `depto` (
  `coddepto` int NOT NULL AUTO_INCREMENT,
  `nomedepto` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci NOT NULL,
  PRIMARY KEY (`coddepto`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `depto`
--

LOCK TABLES `depto` WRITE;
/*!40000 ALTER TABLE `depto` DISABLE KEYS */;
INSERT INTO `depto` VALUES (1,'Ciências'),(2,'Línguas'),(3,'Vagabundagem');
/*!40000 ALTER TABLE `depto` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `disciplina`
--

DROP TABLE IF EXISTS `disciplina`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `disciplina` (
  `coddepto` int NOT NULL,
  `numdisc` int NOT NULL,
  `nomedisc` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci NOT NULL,
  `creditosdisc` int NOT NULL DEFAULT '10',
  PRIMARY KEY (`coddepto`,`numdisc`),
  CONSTRAINT `disciplina_FK` FOREIGN KEY (`coddepto`) REFERENCES `depto` (`coddepto`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `disciplina`
--

LOCK TABLES `disciplina` WRITE;
/*!40000 ALTER TABLE `disciplina` DISABLE KEYS */;
INSERT INTO `disciplina` VALUES (1,1,'Física',10),(1,2,'Química',10),(1,3,'Biologia',10),(2,1,'Inglês',10),(2,2,'Português',10),(2,3,'Grego',10),(3,1,'DCE',10);
/*!40000 ALTER TABLE `disciplina` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `horario`
--

DROP TABLE IF EXISTS `horario`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `horario` (
  `coddepto` int NOT NULL,
  `numdisc` int NOT NULL,
  `anosem` int NOT NULL,
  `siglatur` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci NOT NULL,
  `diasem` int NOT NULL,
  `horainicio` time NOT NULL,
  `codpredio` int NOT NULL,
  `numsala` int NOT NULL,
  `numhoras` int NOT NULL DEFAULT '2',
  PRIMARY KEY (`coddepto`,`numdisc`,`anosem`,`siglatur`,`diasem`,`horainicio`),
  KEY `horario_FK_1` (`codpredio`,`numsala`),
  CONSTRAINT `horario_FK` FOREIGN KEY (`coddepto`, `numdisc`, `anosem`, `siglatur`) REFERENCES `turma` (`coddepto`, `numdisc`, `anosem`, `siglatur`),
  CONSTRAINT `horario_FK_1` FOREIGN KEY (`codpredio`, `numsala`) REFERENCES `sala` (`codpredio`, `numsala`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `horario`
--

LOCK TABLES `horario` WRITE;
/*!40000 ALTER TABLE `horario` DISABLE KEYS */;
INSERT INTO `horario` VALUES (1,1,202301,'fis2023011',1,'08:00:00',1,1,2),(1,1,202301,'fis2023012',1,'10:00:00',1,1,2),(1,2,202301,'qui2023011',1,'13:00:00',1,1,2),(1,2,202301,'qui2023012',1,'08:00:00',1,2,2),(1,3,202301,'bio2023011',1,'10:00:00',1,2,2),(1,3,202301,'bio2023012',1,'13:00:00',1,2,2),(2,1,202301,'ing2023011',1,'15:00:00',1,2,2),(2,1,202301,'ing2023012',1,'08:00:00',2,1,2),(2,2,202301,'por2023011',1,'10:00:00',2,1,2),(2,2,202301,'por2023012',1,'13:00:00',2,1,2),(2,3,202301,'gre2023011',1,'15:00:00',2,1,2),(2,3,202301,'gre2023012',1,'08:00:00',2,2,2),(3,1,202301,'vag2023011',1,'10:00:00',2,2,2),(3,1,202301,'vag2023012',1,'13:00:00',2,2,2);
/*!40000 ALTER TABLE `horario` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `predio`
--

DROP TABLE IF EXISTS `predio`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `predio` (
  `codpredio` int NOT NULL AUTO_INCREMENT,
  `descricaopredio` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci NOT NULL,
  PRIMARY KEY (`codpredio`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `predio`
--

LOCK TABLES `predio` WRITE;
/*!40000 ALTER TABLE `predio` DISABLE KEYS */;
INSERT INTO `predio` VALUES (1,'Grandão'),(2,'Pequenino'),(3,'Médio'),(4,'Adminstrativo'),(5,'Financeiro');
/*!40000 ALTER TABLE `predio` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `prereq`
--

DROP TABLE IF EXISTS `prereq`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `prereq` (
  `coddepto` int NOT NULL,
  `numdisc` int NOT NULL,
  `coddeptoprereq` int NOT NULL,
  `numdiscprereq` int NOT NULL,
  PRIMARY KEY (`coddepto`,`numdisc`,`coddeptoprereq`,`numdiscprereq`),
  CONSTRAINT `prereq_FK` FOREIGN KEY (`coddepto`, `numdisc`) REFERENCES `disciplina` (`coddepto`, `numdisc`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `prereq`
--

LOCK TABLES `prereq` WRITE;
/*!40000 ALTER TABLE `prereq` DISABLE KEYS */;
INSERT INTO `prereq` VALUES (2,1,2,2),(2,3,2,2);
/*!40000 ALTER TABLE `prereq` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `professor`
--

DROP TABLE IF EXISTS `professor`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `professor` (
  `codprof` int NOT NULL AUTO_INCREMENT,
  `coddepto` int NOT NULL,
  `codtit` int NOT NULL,
  `nomeprof` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci NOT NULL,
  PRIMARY KEY (`codprof`),
  KEY `professor_FK` (`coddepto`),
  KEY `professor_FK_1` (`codtit`),
  CONSTRAINT `professor_FK` FOREIGN KEY (`coddepto`) REFERENCES `depto` (`coddepto`),
  CONSTRAINT `professor_FK_1` FOREIGN KEY (`codtit`) REFERENCES `titulacao` (`codtit`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `professor`
--

LOCK TABLES `professor` WRITE;
/*!40000 ALTER TABLE `professor` DISABLE KEYS */;
INSERT INTO `professor` VALUES (1,1,1,'Tavares'),(2,1,2,'Kânia'),(3,2,1,'ZimTom'),(4,2,2,'Dóia'),(5,3,3,'Podôd');
/*!40000 ALTER TABLE `professor` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `profturma`
--

DROP TABLE IF EXISTS `profturma`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `profturma` (
  `coddepto` int NOT NULL,
  `numdisc` int NOT NULL,
  `anosem` int NOT NULL,
  `siglatur` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci NOT NULL,
  `codprof` int NOT NULL,
  PRIMARY KEY (`coddepto`,`numdisc`,`anosem`,`siglatur`,`codprof`),
  KEY `profturma_FK` (`codprof`),
  CONSTRAINT `profturma_FK` FOREIGN KEY (`codprof`) REFERENCES `professor` (`codprof`),
  CONSTRAINT `profturma_FK_1` FOREIGN KEY (`coddepto`, `numdisc`, `anosem`, `siglatur`) REFERENCES `turma` (`coddepto`, `numdisc`, `anosem`, `siglatur`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `profturma`
--

LOCK TABLES `profturma` WRITE;
/*!40000 ALTER TABLE `profturma` DISABLE KEYS */;
INSERT INTO `profturma` VALUES (1,1,202301,'fis2023011',1),(1,1,202301,'fis2023012',1),(1,2,202301,'qui2023011',1),(1,2,202301,'qui2023012',2),(1,3,202301,'bio2023011',2),(1,3,202301,'bio2023012',2),(2,1,202301,'ing2023011',3),(2,1,202301,'ing2023012',3),(2,2,202301,'por2023011',3),(2,2,202301,'por2023012',4),(2,3,202301,'gre2023011',4),(2,3,202301,'gre2023012',4),(3,1,202301,'vag2023011',5),(3,1,202301,'vag2023012',5);
/*!40000 ALTER TABLE `profturma` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `sala`
--

DROP TABLE IF EXISTS `sala`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `sala` (
  `codpredio` int NOT NULL,
  `numsala` int NOT NULL,
  `descricaosala` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci NOT NULL,
  `capacidade` int NOT NULL DEFAULT '70',
  PRIMARY KEY (`codpredio`,`numsala`),
  CONSTRAINT `sala_FK` FOREIGN KEY (`codpredio`) REFERENCES `predio` (`codpredio`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `sala`
--

LOCK TABLES `sala` WRITE;
/*!40000 ALTER TABLE `sala` DISABLE KEYS */;
INSERT INTO `sala` VALUES (1,1,'S1P1',70),(1,2,'S2P1',70),(1,3,'S3P1',70),(1,4,'S4P1',70),(1,5,'S5P1',70),(1,6,'S6P1',70),(1,7,'S7P1',70),(1,8,'S8P1',70),(1,9,'S9P1',70),(1,10,'S10P1',70),(2,1,'S1P2',70),(2,2,'S2P2',70),(2,3,'S3P2',70),(3,1,'S1P3',70),(3,2,'S2P3',70),(3,3,'S3P3',70),(3,4,'S4P3',70),(3,5,'S5P3',70),(4,1,'S1P4',70),(4,2,'S2P4',70),(4,3,'S3P4',70),(4,4,'S4P4',70),(5,1,'S1P5',70);
/*!40000 ALTER TABLE `sala` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `titulacao`
--

DROP TABLE IF EXISTS `titulacao`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `titulacao` (
  `codtit` int NOT NULL AUTO_INCREMENT,
  `nometit` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci NOT NULL,
  PRIMARY KEY (`codtit`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `titulacao`
--

LOCK TABLES `titulacao` WRITE;
/*!40000 ALTER TABLE `titulacao` DISABLE KEYS */;
INSERT INTO `titulacao` VALUES (1,'Mestre'),(2,'Doutor'),(3,'Vagabundo');
/*!40000 ALTER TABLE `titulacao` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `turma`
--

DROP TABLE IF EXISTS `turma`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `turma` (
  `coddepto` int NOT NULL,
  `numdisc` int NOT NULL,
  `anosem` int NOT NULL,
  `siglatur` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci NOT NULL,
  `capacidade` int NOT NULL DEFAULT '50',
  PRIMARY KEY (`coddepto`,`numdisc`,`anosem`,`siglatur`),
  CONSTRAINT `turma_FK` FOREIGN KEY (`coddepto`, `numdisc`) REFERENCES `disciplina` (`coddepto`, `numdisc`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `turma`
--

LOCK TABLES `turma` WRITE;
/*!40000 ALTER TABLE `turma` DISABLE KEYS */;
INSERT INTO `turma` VALUES (1,1,202301,'fis2023011',50),(1,1,202301,'fis2023012',50),(1,2,202301,'qui2023011',50),(1,2,202301,'qui2023012',50),(1,3,202301,'bio2023011',50),(1,3,202301,'bio2023012',50),(2,1,202301,'ing2023011',50),(2,1,202301,'ing2023012',50),(2,2,202301,'por2023011',50),(2,2,202301,'por2023012',50),(2,3,202301,'gre2023011',50),(2,3,202301,'gre2023012',50),(3,1,202301,'vag2023011',50),(3,1,202301,'vag2023012',50);
/*!40000 ALTER TABLE `turma` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Dumping events for database 'teste_analista_dados_questao_2'
--

--
-- Dumping routines for database 'teste_analista_dados_questao_2'
--
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2025-12-15 11:44:57
