"""
test transformer
"""

from dags.ods.kktix_ticket_orders.udfs.kktix_transformer import transform


def test_transform(kktix_api_data) -> None:
    ground_truth = [
        {
            "id": 84296,
            "name": "PyCon APAC 2022 Registration: Individual【Online Conference】",
            "attendee_info": {
                "id": 84748358,
                "ticket_id": 449116,
                "ticket_name": "Regular 一般票（with Pyckage）",
                "reg_no": 104,
                "state": "activated",
                "checkin_code": "BC7B",
                "qrcode": "bc7bd846f49d2d2e1g833cc92gdg2cf9",
                "is_paid": True,
                "price": 2600,
                "currency": "TWD",
                "payment_method": "WEBSITE",
                "data": [
                    ["Nickname / 暱稱", "Stanley"],
                    ["Gender / 生理性別", "Male / 男性"],
                    [
                        "If you buy the ticket with PySafe, remember to fill out correct address and size of t-shirt for us to send the parcel. if you fill the wrong information to cause missed delivery, we will not resend th",
                        "",
                    ],
                    [
                        "購買含 Pyckage 票卷者，請務必填寫正確之「Address / 收件地址」和「Size of T-shirt / T恤尺寸 」（僅限台灣及離島區域），以避免 Pyckage 無法送達，如因填寫錯誤致未收到 Pyckage ，報名人須自行負責，大會恕不再另行補寄",
                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    ],
                    [
                        "Address / 收件地址  Ex: No. 128, Sec. 2, Academia Rd., Nangang Dist., Taipei City 115201, Taiwan (R.O.C.) / 115台北市南港區研究院路二段128號",
                        "36190b79eb7396cfb91e413fecef9707bca87f32012fb01fc38caa236fb053d0",
                    ],
                    [
                        "Size of T-shirt / T恤尺寸",
                        "M / 胸寬(F.W.): 49cm / 衣長(C.L.): 70cm",
                    ],
                    ["Come From / 國家或地區", "Taiwan 台灣"],
                    ["Age range / 年齡區間", "36 - 45"],
                    [
                        'Job Title / 職稱 (If you are a student, fill in "student")',
                        "全端工程師",
                    ],
                    [
                        "Company  / 服務單位 (For students or teachers, fill in the School + Department Name)",
                        "雲灣資訊有限公司",
                    ],
                    ["Years of Using Python / 使用 Python 多久", "6-10 years"],
                    [
                        "Area of Interest / 興趣領域",
                        "Web Development, DevOps, Engineering & Mathematics",
                    ],
                    [
                        "Have you ever attended PyCon TW？/ 是否曾參加 PyCon TW？",
                        "5-7 times",
                    ],
                    [
                        "Would you like to receive an email from sponsors？/ 是否願意收到贊助商轉發 Email 訊息？",
                        "Yes",
                    ],
                    [
                        "I would like to donate invoice to Open Culture Foundation / 我願意捐贈發票給開放文化基金會 (ref: https://reurl.cc/ZQ6VY6)",
                        "No",
                    ],
                    [
                        "Privacy Policy of PyCon APAC 2022 / PyCon APAC 2022 個人資料保護聲明",
                        "",
                    ],
                    [
                        "I’ve already read and I accept the Privacy Policy of PyCon APAC 2022 / 我已閱讀並同意 PyCon APAC 2022 個人資料保護聲明",
                        "Yes",
                    ],
                    [
                        "I am fully aware of the Gather Privacy Policy,  only participants that are over the age of 18 can access to the venue / 我已被告知因為 gather 政策，需滿18歲以上方能登入會議場地",
                        "",
                    ],
                    [
                        "聯絡人 姓名",
                        "2150750f32ee8dcd40537be8b5bee7c26e893a77cb23049eb3a0ca49a7512791",
                    ],
                    [
                        "聯絡人 Email",
                        "26a695fcd9d98ffa1fba78cb5a1eacf0fbe19e40bf9de0cafa0080cdf4c14514",
                    ],
                    [
                        "聯絡人 手機",
                        "86f3abfffd2f714a6d611429f82fac9264e8036b0fb490320bfe3e56c494a0e0",
                    ],
                    ["標籤", ""],
                ],
                "kyc": {},
                "id_number": None,
                "updated_at": 1656921502.5667331,
                "ticket_type": "qrcode",
                "slot": {},
                "order_no": 127666621,
            },
        }
    ]
    if __debug__:
        if transform(kktix_api_data) != ground_truth:
            raise AssertionError(
                "Transform() might forget to de-identify some columns! e.g. name, email or phone number"
            )
