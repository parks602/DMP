Cauly DMP 런덱 실행 프로세스
•	프로세스 정의
•	세부 설명 
  o	1. Cauly DMP dict 생성
  o	2. Cauly custom dmp table 생성
  o	3. Cauly dmp table 생성
프로세스 정의
  1.	cauly dmp dictionary 생성
  2.	custom dmp dictionary & table 생성
  3.	cauly dmp table 생성
 
세부 설명
1. Cauly DMP dict 생성
  •	목표
    o	cauly_dmp_dict 테이블을 생성하며, cauly dmp에 필요한 모든 테이블의 segment들을 통합하며, segment간 중복을 제거하고 유일성을 만들고자 새로운 segment를 부여한다.
  •	생성 테이블
    o	cauly_dmp_dict
  •	참조 테이블
    o	persona_count
    o	cauly_predict_segments
    o	offline_dmp_by_geo_location
    o	rt_user_activity_daily3
    o	prediction_carrier
2. Cauly custom dmp table 생성
  •	목표
    o	streamlit에서 생성되는 custom segment의 모수(scode)를 찾아 cauly custom table에 적재하는 프로세스
  •	노트북을 보면 함수로 잘 정리되어 있고 함수 진행으로 모든 코드가 구성되어 있습니다. 따라서 각 함수에 대한 간략한 설명 첨부합니다.
    o	make_custom_dmp()
      	cauly custom dmp 테이블 생성 함수
    o	get_custom_dict()
      	streamlit에서 생성되는 segment dictionary 조회. segment 생성 방식이 cualy_dmp를 이용하는 것과 query를 이용하는 것으로 나누어져 2번 실행이 필요함(입력 변수인 table name을 아래 참조 테이블 활용)
    o	make_custom_dmp_dmp()
      	cauly_dmp를 활용한 segment의 scode값 생성
    o	make_custom_dmp_query()
      	query를 활용한 segment의 scode값 생성
  •	생성 테이블
    o	cauly_custom_dmp
  •	참조 테이블
    o	custom_cauly_dmp_dict_with_cauly_dmp
    o	custom_cauly_dmp_dict_with_query
3. Cauly dmp table 생성
  •	목표
    o	1, 2에서 생성된 cauly_dmp_dict와 cauly_custom_dmp를 활용해 cauly_dmp 테이블을 생성하고 데이터를 적재
  •	생성 테이블
    o	cauly_dmp
  •	참조 테이블
    o	rt_user_activity_daily3
    o	persona_count
    o	offline_dmp_by_geo_location
    o	cauly_predict_segments
    o	prediction_carrier
    o	cauly_dmp_dict
