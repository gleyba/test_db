use test_db::csv_utils::*;
use test_db::query::*;
use test_db::query_processor::*;
use test_db::record::*;

use flexbuffers::VectorReader;
use mimalloc::MiMalloc;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const TEST_DATA: &'static [u8] = b"Donor ID,Donor City,Donor State,Donor Is Teacher,Donor Zip
00000ce845c00cbf0686c992fc369df4,Evanston,Illinois,No,602
00002783bc5d108510f3f9666c8b1edd,Appomattox,other,No,245
00002d44003ed46b066607c5455a999a,Winton,California,Yes,953
00002eb25d60a09c318efbd0797bffb5,Indianapolis,Indiana,No,462
0000300773fe015f870914b42528541b,Paterson,New Jersey,No,75
00004c31ce07c22148ee37acd0f814b9,,other,No,
00004e32a448b4832e1b993500bf0731,Stamford,Connecticut,No,69
00004fa20a986e60a40262ba53d7edf1,Green Bay,Wisconsin,No,543
00005454366b6b914f9a8290f18f4aed,Argyle,New York,No,128
0000584b8cdaeaa6b3de82be509db839,Valparaiso,Indiana,No,463
00005f52c98eeaf92b2414a352b023a4,Villanova,Pennsylvania,No,190
00006084c3d92d904a22e0a70f5c119a,Brick,New Jersey,Yes,87
00006c6b8c3225a54438f878d59e650a,Wilmington,Delaware,No,198
0000812bd5629117f8909f73acbe8b7d,Pasadena,Texas,No,775
0000889adf4cc958a35daee1f2529b48,Mohegan Lake,New York,No,105
00008eec5aab2228652e22457881f2d0,Old Fort,North Carolina,No,287
0000954e7c49ebfbcd91ed9052070bee,Quincy,Illinois,No,623
0000a1288b8ccdeaaf716a2480d7b06a,,other,No,
0000a2175753bc165e53c408589a3bd6,Grand Rapids,Michigan,No,495
0000a3fd8b8a3d1a90fbb1e0cd44c62b,Lancaster,Pennsylvania,No,176
0000a558c8a53a7e8d66d208a373470e,Fort Washington,Maryland,No,207
0000a9af8b6b9cc9e41f53322a8b8cf1,East Stroudsburg,New York,Yes,183
0000b282b7070b4dd31c05210f82453b,Ocala,Florida,No,344
0000bbd74feb563a324fe441eae19feb,Indianapolis,Indiana,No,462
0000bc074e37420d6280c09abe129966,,New Jersey,No,
0000bc6a51a9c31629d70f290944ba0a,Honolulu,Hawaii,No,968
0000be4b3c81e1cef858d536bb740052,Circle Pines,Minnesota,No,550
0000c14308c4cb9259a4fe51f692c9ef,Brooklyn,New York,No,112
0000c20705a45563f2ec6a53088c2a30,Laurens,South Carolina,No,293
0000c4be79d9f92485829c9e275890b7,Molalla,Oregon,No,970
0000c93962b49ca4cb87e0632a132c2c,Smithville,Mississippi,No,388
0000cce04fec25bf7f21b0e2f1dcf4b6,Lowell,Michigan,No,493
0000d299ce46c8375f29f7bb792b9eae,Louisville,Kentucky,No,402
0000d2c093a6301ef33925c06af2c6d1,,other,No,
0000d4777d14b33a1406dd6c9019fe89,,other,Yes,
0000d72a5299c2fe5a867e31f0b61c46,,other,No,
0000e1b68421441f1bb8e697ecdad119,Raleigh,North Carolina,No,276
0000ee82242add1f15f823c24833359f,,Texas,No,
0000fc11407901bcacdfad1db909b9f6,,other,Yes,
00010615b56ff057fa00b5144fe2e4cf,Los Angeles,California,No,900
000107cdc0ebf5aa274c12837cf16de7,Trinity,Alabama,No,356
0001107b9faa5c3bb42cfcecece1d587,Portland,Oregon,No,972
000110c8c5db5d760b313292dbfb24d1,Tempe,Arizona,No,852
0001159d13601a582dda846f252ec4c1,Warren,Michigan,No,480
00011c48767cf67073b1f3bcc91dc7dc,Cary,North Carolina,No,275
00011e359255acd86be0e56f6b62f1b0,Stuart,Florida,No,349
00012258ed40e698a7c66ba144057392,Ishpeming,Michigan,No,498
000126642ada6832f40fd322b35d8036,Middletown,New York,No,109
00012c73606c2394e5f3c76fbd0a143c,Franklin,Maryland,No,480
00013aca9874e0c2ea6a13a949a41f17,Indianapolis,Indiana,No,462
0001415ef50c0f45d066c97e69489c1f,Houston,Texas,No,770
00014d846426ac502c555c2c28a0ef63,Sioux City,Iowa,No,511
000150db429aff026ecd130b0a076cef,Gilberts,Illinois,No,601
00015a610769900026131aacfdd1bc62,Havana,Florida,No,323
00015a70349de4732f7e113934a30b67,Ponte Vedra Beach,Florida,No,320
0001645e0c9384665108dc2cb38ad1ca,Many,Louisiana,No,714
00016d53aafa42950e88f5996fd69e0a,,North Carolina,No,
00016efa41348375e57a3bc1270114f9,Merchantville,New Jersey,No,81
000177bef7ed7b7d1d0f5741d0b5fab8,Diamond Bar,California,No,917
00017bad30d4b5d991b81b04cb4988b8,Los Gatos,California,No,950
00017ccecdbe0fe4c997bd7846346411,Dayton,Nevada,No,894
000181f354f1cba54f958463835ced41,Fullerton,California,No,928
00019e1dcd80085636a622f27c5b1233,New York,New York,No,100
0001aa9a0443a0bcd6e98619b44ee42d,Bolingbrook,Illinois,No,604
0001abd0c3f256bcdbc75116f2609355,Canyon Country,California,No,913
0001b46c36c7c900cecd5011f7f3e6d6,Chicago,Illinois,No,606
0001bd04bff7f57077f5532c7c73b388,Webster,Texas,No,775
0001bde8e87c867f3d449edfcb47765e,Yorba Linda,California,No,928
0001c6641864a240eff74544a3596acf,Danvers,Pennsylvania,Yes,19
0001d30db838865788098cf241501ea0,Chicago,Illinois,No,606
0001d85b5295f830da61a4acefae8dde,Miami,Florida,No,331
0001daec1f7799df50f1e4e05deb4e66,Myrtle Beach,South Carolina,No,295
0001ef9f64a7e1038e0811766c25e6f8,Ames,Iowa,No,500
0001f63e9437ebbba6ddaae0664037a7,Oklahoma City,Oklahoma,Yes,731
0001f79bb789fa9c83b13ef99f7bbe6e,Ridgewood,Ohio,No,113
0002021bb799f28de224f1acc1ff08c4,Centerville,Massachusetts,No,26
000207b8d3d67c12f7711511f62cf9d4,Evans,Georgia,No,308
000210a2a948e929d8e04897dc921d91,Georgetown,Delaware,No,199
0002128c613edd04baf344ef01e362c9,Vallejo,California,No,945
0002186e790ca5590203a1dc5318915f,Lakewood,Ohio,No,441
000226bf2362b3ea92229b29c2edf737,Saint Louis,Missouri,Yes,631
00022a0f4f0062d861b26fcd96abc68c,Deerfield,Ohio,No,444
000233fb8cbd3fcd20304423156cdf39,Seattle,Washington,Yes,981
0002348747db8f7fcf05be91399c5707,Denver,Colorado,No,802
000238615daaeaf201582eb3e13ab9cd,Lone Tree,Colorado,No,801
0002413480f8e6a120e3927f095f7e59,Walla Walla,Washington,No,993
00024e86676fc2c3b317e0166ffa4768,Ann Arbor,Michigan,No,481
0002555bbe359440d6ceb34b699d3932,Portland,Maine,No,41
000267b5672a90ea8e6d4e485a63d3d6,Newark,Delaware,No,197
00026986208d02b67d03a230228bb405,Stone Mountain,Georgia,No,300
00026a74542793470c245995ab622f96,Baytown,Texas,No,775
00027d3b680199e9350bc20fd2454a02,Old Forge,Pennsylvania,No,185
0002806acba3480d65cec587c1afb1ee,,other,Yes,
000281260e901bdc9ce5ec8b957dd5ad,Greenville,North Carolina,No,278
00028d5d75335732b1c46c54c5c847dd,Loxley,Alabama,No,365
00028e5aea55cf68a3216cd74ecceff1,Lakeville,Massachusetts,No,23
00028fae880618d73d2dcfa3e5bc670b,Moorhead,Minnesota,No,565
0002901d0d091e95801ed750df6720b2,Chicago,Illinois,Yes,606
0002965240b13a26f348742cbae55c0c,Washington,District of Columbia,No,200
00029725138190e59f0536088be7d1db,Medway,Massachusetts,No,20
000297cd8e13cb1e364990075c792e2f,Clifton,New Jersey,No,70
00029c282384a5d15bf2184b0578630e,Oklahoma City,Oklahoma,No,731
0002a1159a1b2a36c51d4e524c4e16f4,Fountaintown,Indiana,No,461
0002a45d0b45a78e9c920beba40bc7fb,Ann Arbor,Michigan,No,481
0002aac1c9addac1d4ee6902d6ec5404,Oxon Hill,Maryland,No,207
0002ac317113dd9faac7fa9d0d1ba2ac,Natick,Massachusetts,No,17
0002af226b13fc8b4310993d6a5d1386,Brooklyn,New York,No,112
0002b4e02d351c8719487ce7c02583a0,Falcon,North Carolina,No,283
0002b674d10ab80873a6665b0e472dce,Sabina,Ohio,No,451
0002bcb9f69cd91cbeae163fc9cd874f,Ponca City,Oklahoma,Yes,746
0002c4d61c342b774712dfc6d53b77f8,Chicago,Illinois,No,606
0002c51ea63953674cb368b994ebaeb7,Lafayette,Louisiana,No,705
0002c6eb1c6987c8a44adb715e11cd95,O Fallon,Illinois,No,622
0002cb56c84b1cba7de9294020eda991,Avon,Connecticut,No,60
0002d6d3b9886f0e360f8d829870f759,,Arizona,No,
0002d777804d485adbcca7aad4ad96c5,Warren,Michigan,Yes,480
0002e30532c02dbc57e35b4bdef3933d,,other,No,
0002e7d0648c093bf75829e52c1ea21a,Helena,Alabama,No,350
0003079fefc25462d7dffdd86e17181a,Chandler,Texas,No,757
00031a7e84cb620d9f78bf1b42ea1d31,Berkeley,California,No,947
00031ee2136eb183079ced8d13e678ef,Lucedale,Mississippi,Yes,394
0003232e650202eb576b93e1e5947c59,Towson,Maryland,No,212
00032349e9b32f61f47abf82fda1f3d1,Sunland,California,No,910
000328171351a6e51177c89f068e61f0,,Georgia,No,
00032a6160b84939c66d364c575fe881,Ronkonkoma,New York,No,117
00033085be55865b54fa35cb46138ee6,Fort Smith,Arkansas,No,729
000335d76fd3532adfe8bacda134b043,Chicago,Illinois,No,606
00036923e5f5786c68551769bfc24b9c,Lynn,Massachusetts,No,19
0003733341da1a9a83b472b92a5e6de8,Pewaukee,Wisconsin,No,530
0003748b6011978a14b4bbb0c0a2f4bd,Sacramento,California,No,958
00037c0f2464ae6b9b122898942f8110,Willington,Connecticut,No,62
00038313e9d8b0d93ab8e6b9d66bac70,Oakley,California,No,945
0003982f3e5d7e11e4dbdc8ab26648be,Seattle,Washington,No,981
00039eb54d35f2104e024d21d9168f67,Ridgewood,New York,No,113
0003aba06ccf49f8c44fc2dd3b582411,,Pennsylvania,No,183
0003b11dff71ce61ee9399fdc3660a1d,Borden,Indiana,No,471
0003b4922837de5939739ca3f3bb7ec0,,New York,No,
0003f5b917fbd5ab4b762235fb8c0978,Copperas Cove,Texas,No,765
0003f6dbeb4660decee100b2308f91ab,Pittsburgh,Pennsylvania,Yes,152
0004022cc26b79c37471e953f521d0dc,Fort Lauderdale,Florida,No,333
00040a049ad9f348f841cc4287cb24a6,Hayward,California,Yes,945
0004113dff833671f531e7f85445d9c7,Tucker,Georgia,No,300
00041b6adb7fa596519eccbd354c89d9,,other,No,
00041fc8b829a81356aa53013410095d,Rio Linda,California,No,956
0004265c44e425d7104e2d0fb8ecd5c9,Chico,California,No,959
0004298ea9ff1bf0dd5a1c154dcd4b36,Los Angeles,California,No,900
00042f7b46b75d493b33e9b0b5c43d87,Portland,Oregon,No,972
00043515876762727a871c9c83ff525f,Columbia,Missouri,No,652
0004416c6745afb13fe63ef0d45f3acb,Grand Blanc,Michigan,Yes,484
000446688c194fc58e22412ff3a696b1,Smyrna,Georgia,No,300
00044968bb11dd7b687915dc48c750f4,Cheyenne,Wyoming,No,820
000453f0866b2875d77f563d1aafb16e,Springfield,Oregon,No,974
00046271d980cfd254dbff50311976b0,Minneapolis,Minnesota,No,554
000468d6e7fee14bfb4f37e6abd14d58,Pawleys Island,South Carolina,No,295
00046dc723c15f748b48b0d408f167f7,Seffner,Florida,No,335
000475c3717556a33ecb54772dac9db5,South Pittsburg,Tennessee,Yes,373
000476bf6584f0ee3ea6e94b99d673e6,Waltham,Massachusetts,Yes,24
00047ac546738c937a13675b35584f28,San Francisco,California,No,958
000490b9ff5d803309376944dbbb2cd5,Blanchard,Oklahoma,No,730
0004914aca77549c469cce0defa909f8,Belleville,Illinois,No,622
00049338d2a420cd94df66e52af88547,Los Molinos,California,No,960
0004a1e99046415fccd166fcbf65f156,Washington,District of Columbia,No,200
0004be01ccfd90c20c542fe213b1e6aa,Daly City,California,No,940
0004c0379906e29420580163bf5b2c37,Seattle,Washington,No,981
0004c11d84e7e1fc83a72e0f790cd7d8,Lugoff,South Carolina,No,290
0004c4500d7e39380bfa9ba4905bc4f8,Charlotte,North Carolina,No,282
0004ceb1d06fd98f0ba0364cbd7f8bdc,San Francisco,California,No,941
0004d30ae551f626ce3810d3883903c2,Columbia,North Carolina,Yes,279
0004daeb02f866d0b146da3e1db8e812,La Vergne,Tennessee,No,370
0004e6f8348593dbf0ae50ea31d9b0a6,Brookfield,Wisconsin,No,530
0004ef5c2dd39bb1f4876370773659ef,,other,No,
0004eff0774c68fabaec34e3328a4928,Saint James,Minnesota,No,560
0004f42bf17585076f354d58485129be,Muskegon,Michigan,No,494
0004fe043a49093ddd494e630f9ef512,Jacksonville,Illinois,Yes,626
0004ffe3558fd70d939ad522b92447c8,Edmond,Oklahoma,No,730
0005007d150e1462082c900247dd9a6a,,Texas,No,
00050297e37eb7632d4b9ebd80ef5924,Austin,Texas,No,787
00050511b93260bed3152f1fa22bf4b3,Jefferson,Georgia,No,305
000508bc79d28e7aa80ff6597f4f3db7,Melissa,Texas,No,754
00050a062e1548d98c7af7f44309154e,Suffolk,Virginia,No,234
00050f203b55e72fc8ada0319813ed32,Chula Vista,California,No,919
00050f2e6d89737c5d2af615f9aaf32b,,Oregon,Yes,
00051e4d6cacc63f05442ef9bffa1310,Greensboro,North Carolina,No,274
00052006c47375049337beafe2a2308b,Saint Paul,Minnesota,No,551
0005217360eaad75db1d28837bd15658,Hoboken,New Jersey,No,70
0005218c60733bc970b7e703a0dfe57f,Savannah,Georgia,No,314
000529aa89291c6825144b8dbbd399ab,Urbana,Illinois,No,618
00052b8a1c5eab1a8086c2577d7f19dc,Clayton,North Carolina,No,275
0005327bfe18229b926c7d15aa2eb32b,Acton,California,No,935
00053c6a9d9bfdfccc009c642983ebb0,Gulf Breeze,Florida,No,325
00053d1eadab5899fb359b14dfc5a00c,Moorhead,Minnesota,No,565
000542d72c374fc65c58a79420078a55,Valparaiso,Indiana,No,463
00054868052316310fc41e42eafe3ecd,McKinney,Texas,No,750
00054f4b278af0c8ded9e9449339340a,American Fork,Utah,No,840
0005546c587642c3ade7be5fe4ecd640,Minneapolis,Minnesota,No,554
0005563d86efe412de0654a9c15b8b7e,New Orleans,Louisiana,No,701
00055ed4f4745e71dbb25b0b2b40f306,Albuquerque,New Mexico,No,871
000562cc943f4bb8aa6db6cd9baeefcf,,California,No,
0005788cc0356d42c93193917b402967,Canoga Park,California,No,913
000581cf61255745a0a6789c5d2ad072,Ankeny,Iowa,No,500
000583fdc4983283e74c3397102f9af6,Agawam,Massachusetts,No,10
0005b3d2dfa15c12fc258071d3b47020,Jacksonville,Florida,No,322
0005bb4838787c4ed1475f742c4cdd36,Florence,South Carolina,No,295
0005bcf354beaaf07e1d4aafbbe97032,Howard,Colorado,No,812
0005bcf3896a09114d7d6077efa43924,Pasadena,Texas,No,775
0005bcfc6ff3fac055969572c677fda7,Wylie,Texas,No,750
0005bf4d6818b8bbbc858c57887605bf,Lake Zurich,Illinois,No,600
0005d20121983a3e7daaaad30759f8b7,Fairfield,California,No,945
0005d3a0de873181b9feeb4b4d762254,,other,No,
0005ddbf06bdba7d62bb999b512c7844,Princeton,Louisiana,No,710
0005ddde8adf3449484ac8a242c67cc2,Vero Beach,Florida,No,329
0005e94def8dbd5e0ca7a01437536f9a,Oakland,California,No,946
0005f200ba504e18bfa72b751aa94ca4,Olathe,Kansas,No,660
0005fe36450ab0d115effb7030346d66,Jay,Oklahoma,No,743
0005fe4ee8c0133cac16534e2c7a1e48,Vance,Alabama,No,354
0005ffefbb56fc35b8627c73c4c1c6fc,New York,New York,No,100
00061145ab79728aeee206c51959f012,Palm Harbor,Florida,No,346
00061f4cbe3b29b0cbab8f0ef6068100,Oak Lawn,Illinois,No,604
000629d8ca7e807dee47d389b1715bcd,,other,No,
00062c3cb94f5d558a0ecca78c3d5511,Placentia,California,Yes,928
000630ab66464a738cab5036738075df,Panama City,Florida,No,324
000638e6e1f75a810bf08b2889b518ea,Horseheads,New York,No,148
00063ff82898763135371d1052adaf9d,New Haven,Connecticut,No,65
000641544e0a744e13fe58f325ab62cc,Panama City,Florida,Yes,324
00064eac8b3d1f6dea8a07559922ed58,Troy,Michigan,No,480
00064fccec7dc838b10e7aa99a0ebd60,Renton,Washington,No,980
0006658276977eeefb5bc7b0c7399b5b,Marietta,Georgia,No,300
00066ccc94981679216ed6c32d5f57e4,Mobile,Alabama,No,366
000670b87f78416405d696225b1edc24,Brentwood,New York,No,117
00068a74cc0285896a091ac2a4542a77,Saint Louis,Missouri,No,631
000697d997ae059b73b5b3c5c9d6a24e,Michigantown,Indiana,No,460
00069806069c7df36502b8894e6b81b1,Sandy,Utah,No,840
00069eaf8e754d0b8d3c18de202c7ac6,,New York,No,
0006a9135c313033a9a19c25e6d20670,Plymouth,New Hampshire,Yes,32
0006b25b33c313e3c764345e1db5c688,Sugar Land,Texas,No,774
0006c59e2e24920011fbfd274ee7e377,Eudora,Arkansas,No,716
0006c635801c9772f0c82a903c6998f6,Charlotte,North Carolina,No,282
0006cf9f74f9add3c5f7500c2e18b298,Milford,Connecticut,Yes,64
0006d10d9d2af2fa09ed007d742633b1,,other,Yes,
0006d59944b0473be99a01db51f9a528,Chicago,Illinois,No,606
0006d6759c94dbe94f2d4a30b87f4411,Walnut Creek,California,Yes,945
0006efc837b03194c691550bfdadd12b,,Virginia,No,201
0006fb13ba498b452e5ff6078ee1a279,Vancouver,Washington,No,986
000706e6f582029aef44f72af2522248,Downey,California,No,902
00070e08d1da22eda803f96829d83247,Philadelphia,Pennsylvania,No,191
00070fd37d3e8b5b7c08a4a0ba22aba1,Norman,Oklahoma,No,730
00071bfd6e2e1b34a651e63302cce7c7,Oak Lawn,Illinois,No,604
00071ec42078f3966624bc7ce0305410,Philadelphia,Pennsylvania,No,191
0007220b001956e53452bce577d18ffa,Dixfield,Maine,No,42
00072a3616151a38ac844fe0c816ee9e,Atlanta,Texas,No,303
00073384a6b6c790563af323f0eb01b7,Nashville,Tennessee,No,372
000733ae8cc3aabae184ebaf368b5334,Knierim,Iowa,No,505
0007353c5c2753d46cdbe05c55cd7ba4,Sugar Land,Texas,No,774
00074620315b07c21204066686c2ba42,Kuna,Idaho,Yes,836
0007637d3e8ab4f64f1b1fe3a055df4a,Katy,Texas,No,774
00076c7103b5fe57febb9d8c04af4784,Springfield,Illinois,No,627
00076d15de0f8259e918a55b05662a52,,California,No,963
00076f105811206f3d79dd9f1b24aa99,,Ohio,No,
00076f1c154596e59dd04b0adbfe4c95,Dewitt,Michigan,No,488
000777796eb9423e2153edff79eccd78,Dallas,Texas,No,752
00077a8c59b2234413d5a1a23dc70330,Escanaba,Michigan,No,498
0007851fc9a8da3ab5f44c8d2fdaea82,Seattle,other,Yes,981
0007852eba40a5faf79d9fa17074a1bc,Rock Island,Illinois,Yes,612
000788eabed90a42e3c8527053b65919,Edgewater,Maryland,No,210
000796759b25145e0dcb8a0fad33d801,,other,No,
00079b216b9dbdec236401375bf3eda2,Lexington,South Carolina,No,290
00079f480d2573f85edcd4627c930514,New Haven,Connecticut,No,65
0007c051eb47daf3d1307891f0a488b7,Merritt Island,Florida,No,329
0007c587f59cf59cc697f4a541fdb589,Deland,Florida,No,327
0007da4aa7b6124fea6c5fef9999a77d,Waldport,Oregon,No,973
0007e074e91a42c28831907609cbcf38,Summerville,South Carolina,No,294
0007e61b6a753a67031c64c00bff1251,Oak Park,Illinois,No,603
0007f379269c7a5e02bbf568e0d42938,Palm Coast,Florida,No,321
00081f20619895d34482af10e41b0898,Chevy Chase,Maryland,No,208
00081fa471083705cbc01971755c5661,Denver,Colorado,No,802
00082275367b041972b7b133349f66fd,Fernandina Beach,Florida,No,320
00082ce5c018610ecbf7cf57e8a064f2,Jerome,Idaho,No,833
000831b891e549c18b67c6984f2fbb7e,Berkeley,California,No,947
000832bc0aafcaf274db0db25e4f2bb0,Saint Paul,Minnesota,No,551
000847592e3ce9b193c523e68daaf75a,Seattle,Washington,No,981
0008648dfdc56da51cc8318d28d06a03,Lynnwood,Washington,No,980
000867a4f4785900772008443e42ed6f,Cumberland,Maryland,No,215
00086e2210bba578da08f55960e945b7,,Virginia,No,
00086e4e19e80f3f17e2f05421549ce1,Albany,New York,No,122
00086ecbab11a691655fb926ba841059,Tampa,Florida,No,336
";

struct ParsedCsv {
    headers: Vec<String>,
    flex: Vec<Vec<u8>>,
}

fn parse_csv_to_flex(data: &'static [u8]) -> ParsedCsv {
    let mut flex = Vec::new();
    let data = bytes::Bytes::from(data);
    let mut reader = CSVImportReader::from_first_chunk(data).unwrap();
    let headers = reader.headers().clone();
    let mut iter = reader.parse_records();
    while let Some(value) = iter.next() {
        flex.push(value.map(|r| r.as_flexbuffer()).unwrap());
    }
    ParsedCsv { headers, flex }
}

fn flex_iter(data: &Vec<Vec<u8>>) -> impl Iterator<Item = VectorReader> {
    data.iter().map(|d| {
        flexbuffers::Reader::get_root(d.as_slice())
            .map(|r| r.as_vector())
            .unwrap()
    })
}

fn process_query<'ret, 'de: 'ret>(
    querystr: &str,
    data: &'de ParsedCsv,
) -> QueryProcessor<'ret, 'de> {
    let query = Query::from_query_str(querystr).unwrap();
    let table = query.get_table_name().unwrap();
    assert_eq!(table.name, "donors");
    assert_eq!(table.alias, Some("donors".to_owned()));
    let mut result = QueryProcessor::new(query, data.headers.clone()).unwrap();
    for r in flex_iter(&data.flex) {
        if !result.process_record(&r).unwrap() {
            break;
        }
    }
    result
}

fn query_result_to_csv_str(processor: &QueryProcessor) -> String {
    let mut result = String::new();
    writeln!(result, "{}", processor.headers_csv()).unwrap();
    let mut iter = processor.iter();
    while let Some(record) = iter.next().unwrap() {
        writeln!(result, "{}", record.to_csv().unwrap()).unwrap();
    }
    result
}

fn print_query_result(processor: &QueryProcessor) {
    println!("{}", processor.headers_csv());
    let mut iter = processor.iter();
    while let Some(record) = iter.next().unwrap() {
        println!("{}", record.to_csv().unwrap());
    }
}

fn get_next_value_at<'de>(iter: &mut RecordIterWrapper<'_, 'de>, idx: usize) -> ValueRef<'de> {
    iter.next().unwrap().unwrap().value_at(idx).unwrap()
}

const SELECT_IDS_QUERY: &str = r#"
SELECT donors."Donor ID"
FROM donors AS donors
"#;

#[test]
fn test_select_ids() {
    let data = parse_csv_to_flex(&TEST_DATA);
    let processor = process_query(SELECT_IDS_QUERY, &data);
    let mut qres_iter = processor.iter();
    flex_iter(&data.flex).for_each(|r| {
        let rec = qres_iter.next().unwrap().unwrap();
        assert_eq!(
            rec.value_at(0).unwrap().as_str().unwrap(),
            r.index(0).unwrap().as_str()
        )
    });
    assert!(qres_iter.next().unwrap().is_none());
}

const SELECT_ALL_QUERY: &str = r#"
SELECT *
FROM donors AS donors
"#;

#[test]
fn test_select_all() {
    let data = parse_csv_to_flex(&TEST_DATA);
    let processor = process_query(SELECT_ALL_QUERY, &data);
    let result = query_result_to_csv_str(&processor);
    assert_eq!(result, String::from_utf8_lossy(&TEST_DATA));
}

const COUNT_ALL_QUERY: &str = r#"
SELECT count(*)
FROM donors AS donors
"#;

#[test]
fn test_count_all() {
    let data = parse_csv_to_flex(&TEST_DATA);
    let processor = process_query(COUNT_ALL_QUERY, &data);
    let mut qres_iter = processor.iter();
    let count = get_next_value_at(&mut qres_iter, 0).as_uint().unwrap();
    assert_eq!(count, 285);
    assert!(qres_iter.next().unwrap().is_none());
}

const SELECT_IDS_LIMIT_QUERY: &str = r#"
SELECT donors."Donor ID"
FROM donors AS donors
LIMIT 100
"#;

#[test]
fn test_select_ids_limit() {
    let data = parse_csv_to_flex(&TEST_DATA);
    let processor = process_query(SELECT_IDS_LIMIT_QUERY, &data);
    let mut qres_iter = processor.iter();
    flex_iter(&data.flex).take(100).for_each(|r| {
        assert_eq!(
            get_next_value_at(&mut qres_iter, 0).as_str().unwrap(),
            r.index(0).unwrap().as_str()
        )
    });
    assert!(qres_iter.next().unwrap().is_none());
}

const COUNT_ALL_QUERY_LIMIT: &str = r#"
SELECT count(*)
FROM donors AS donors
LIMIT 100
"#;

#[test]
fn test_count_all_limit() {
    let data = parse_csv_to_flex(&TEST_DATA);
    let processor = process_query(COUNT_ALL_QUERY_LIMIT, &data);
    let mut qres_iter = processor.iter();
    let count = get_next_value_at(&mut qres_iter, 0).as_uint().unwrap();
    assert_eq!(count, 100);
    assert!(qres_iter.next().unwrap().is_none());
}

const SELECT_IDS_ORDER_BY: &str = r#"
SELECT donors."Donor ID", donors."Donor Zip"
FROM donors AS donors
ORDER BY 2
"#;

#[test]
fn test_select_ids_order_by() {
    let data = parse_csv_to_flex(&TEST_DATA);
    let processor = process_query(SELECT_IDS_ORDER_BY, &data);
    let mut zips: Vec<i64> = flex_iter(&data.flex).map(|r| r.idx(4).as_i64()).collect();
    zips.sort();
    let mut qres_iter = processor.iter();
    zips.iter().for_each(|i| {
        let r = get_next_value_at(&mut qres_iter, 1).as_int().unwrap();
        assert_eq!(r, *i)
    });
    assert!(qres_iter.next().unwrap().is_none());
}

const SELECT_IDS_ORDER_BY_COL_NAME: &str = r#"
SELECT donors."Donor ID", donors."Donor Zip"
FROM donors AS donors
ORDER BY donors."Donor Zip"
"#;

#[test]
fn test_select_ids_order_by_col_name() {
    let data = parse_csv_to_flex(&TEST_DATA);
    let processor = process_query(SELECT_IDS_ORDER_BY_COL_NAME, &data);
    let mut zips: Vec<i64> = flex_iter(&data.flex).map(|r| r.idx(4).as_i64()).collect();
    zips.sort();
    let mut qres_iter = processor.iter();
    zips.iter().for_each(|i| {
        let r = get_next_value_at(&mut qres_iter, 1).as_int().unwrap();
        assert_eq!(r, *i)
    });
    assert!(qres_iter.next().unwrap().is_none());
}

const SELECT_IDS_ORDER_BY_COL_NAME_NO_PROJ: &str = r#"
SELECT donors."Donor ID"
FROM donors AS donors
ORDER BY donors."Donor Zip"
"#;

#[test]
fn test_select_ids_order_by_col_name_no_proj() {
    let data = parse_csv_to_flex(&TEST_DATA);
    let processor = process_query(SELECT_IDS_ORDER_BY_COL_NAME_NO_PROJ, &data);
    let mut ids_by_zip: BTreeMap<i64, HashSet<String>> = BTreeMap::new();
    for r in flex_iter(&data.flex) {
        ids_by_zip
            .entry(r.idx(4).as_i64())
            .or_insert_with(|| Default::default())
            .insert(r.idx(0).as_str().to_owned());
    }
    let mut qres_iter = processor.iter();
    ids_by_zip.values().for_each(|i| {
        for _ in 0..i.len() {
            let r = get_next_value_at(&mut qres_iter, 0)
                .as_str()
                .unwrap()
                .to_owned();

            assert!(i.contains(&r));
        }
    });
    assert!(qres_iter.next().unwrap().is_none());
}

const SELECT_IDS_ORDER_BY_DESC: &str = r#"
SELECT donors."Donor ID", donors."Donor Zip"
FROM donors AS donors
ORDER BY 2 DESC
"#;

#[test]
fn test_select_ids_order_by_desc() {
    let data = parse_csv_to_flex(&TEST_DATA);
    let processor = process_query(SELECT_IDS_ORDER_BY_DESC, &data);
    let mut zips: Vec<i64> = flex_iter(&data.flex).map(|r| r.idx(4).as_i64()).collect();
    zips.sort();
    let mut qres_iter = processor.iter();
    zips.iter().rev().for_each(|i| {
        let r = get_next_value_at(&mut qres_iter, 1).as_int().unwrap();
        assert_eq!(r, *i)
    });
    assert!(qres_iter.next().unwrap().is_none());
}

const SELECT_STATE_GROUP_BY: &str = r#"
SELECT donors."Donor State"
FROM donors AS donors
GROUP BY 1
"#;

#[test]
fn test_select_state_group_by() {
    let data = parse_csv_to_flex(&TEST_DATA);
    let processor = process_query(SELECT_STATE_GROUP_BY, &data);
    let states: HashSet<String> = flex_iter(&data.flex)
        .map(|r| r.idx(2).as_str().to_owned())
        .collect();

    let mut qres_iter = processor.iter();
    for _ in 0..states.len() {
        let state = get_next_value_at(&mut qres_iter, 0)
            .as_str()
            .unwrap()
            .to_owned();

        assert!(states.contains(state.as_str()));
    }

    assert!(qres_iter.next().unwrap().is_none());
}

const SELECT_STATE_GROUP_BY_COL_NAME: &str = r#"
SELECT donors."Donor State"
FROM donors AS donors
GROUP BY donors."Donor State"
"#;

#[test]
fn test_select_state_group_by_col_name() {
    let data = parse_csv_to_flex(&TEST_DATA);
    let processor = process_query(SELECT_STATE_GROUP_BY_COL_NAME, &data);
    let states: HashSet<String> = flex_iter(&data.flex)
        .map(|r| r.idx(2).as_str().to_owned())
        .collect();

    let mut qres_iter = processor.iter();
    for _ in 0..states.len() {
        let state = get_next_value_at(&mut qres_iter, 0)
            .as_str()
            .unwrap()
            .to_owned();

        assert!(states.contains(state.as_str()));
    }

    assert!(qres_iter.next().unwrap().is_none());
}

const SELECT_STATE_GROUP_BY_COUNT: &str = r#"
SELECT count(*), donors."Donor State"
FROM donors AS donors
GROUP BY 2
"#;

#[test]
fn test_select_state_group_by_count() {
    let data = parse_csv_to_flex(&TEST_DATA);
    let processor = process_query(SELECT_STATE_GROUP_BY_COUNT, &data);
    let mut states_counts = HashMap::<String, u64>::new();
    for r in flex_iter(&data.flex) {
        let state = r.idx(2).as_str().to_owned();
        *states_counts.entry(state).or_insert(0) += 1;
    }

    let mut qres_iter = processor.iter();
    for _ in 0..states_counts.len() {
        let r = qres_iter.next().unwrap().unwrap();
        let count = r.value_at(0).unwrap().as_uint().unwrap();
        let state = r.value_at(1).unwrap().as_str().unwrap().to_owned();

        assert_eq!(states_counts[&state], count);
    }

    assert!(qres_iter.next().unwrap().is_none());
}

const TEST_QUERY_1: &str = r#"
SELECT
  `donors`."Donor State" `donors__donor_state`,
  count(*) `donors__count`FROM
  test.donors AS `donors`GROUP BY
  1
ORDER BY
  2 DESC
LIMIT
  10000
"#;

#[test]
fn test_first_query() {
    let data = parse_csv_to_flex(&TEST_DATA);
    let processor = process_query(TEST_QUERY_1, &data);
    assert_eq!(
        processor.headers_csv(),
        "donors__donor_state,donors__count".to_owned()
    );
    let mut states_counts = HashMap::<String, u64>::new();
    for r in flex_iter(&data.flex) {
        let state = r.idx(2).as_str().to_owned();
        *states_counts.entry(state).or_insert(0) += 1;
    }

    let mut prev_count = None;
    let mut qres_iter = processor.iter();
    for _ in 0..states_counts.len() {
        let r = qres_iter.next().unwrap().unwrap();
        let count = r.value_at(1).unwrap().as_uint().unwrap();
        let state = r.value_at(0).unwrap().as_str().unwrap().to_owned();
        if let Some(prev) = prev_count {
            assert!(prev >= count);
        }

        prev_count = Some(count);
        assert_eq!(states_counts[&state], count);
    }

    assert!(qres_iter.next().unwrap().is_none());
}

const TEST_QUERY_2: &str = r#"
SELECT
  count(*) `donors__count`FROM
  test.donors AS `donors`WHERE
  (`donors`."Donor City" = "San Francisco")
LIMIT
  10000
"#;

#[test]
fn test_second_query() {
    let data = parse_csv_to_flex(&TEST_DATA);
    let processor = process_query(TEST_QUERY_2, &data);
    let mut qres_iter = processor.iter();
    let count = get_next_value_at(&mut qres_iter, 0).as_uint().unwrap();
    assert_eq!(count, 2);
    assert!(qres_iter.next().unwrap().is_none());
}
